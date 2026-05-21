"""
Peer Zenoh sem relay.

Este arquivo representa um nó da malha que:
- abre uma sessão Zenoh em modo peer;
- publica heartbeats periodicamente;
- escuta heartbeats de outros peers;
- mantém o status dos peers conhecidos;
- marca peers como OFFLINE quando deixam de enviar heartbeat.
"""

import json
import os
import sys
import threading
import time

import zenoh


HEARTBEAT_INTERVAL_SECONDS = 5
PEER_TIMEOUT_SECONDS = 12
LIVENESS_CHECK_INTERVAL_SECONDS = 2

HEARTBEAT_TOPIC_TEMPLATE = "dsot/{node_id}/heartbeat"
ALL_HEARTBEATS_TOPIC = "dsot/*/heartbeat"

REQUIRED_CONFIG_FIELDS = [
    "node_id",
    "short_id",
    "listen_endpoint",
    "neighbor_endpoints"
]

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"


def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")


def print_startup_banner(node_id):
    clear_terminal()

    print("====================================")
    print(f"          {node_id}")
    print("====================================")
    print("Heartbeat + Liveness - SEM RELAY")
    print("Ctrl+C to quit")
    print("====================================")


def print_usage():
    print(f"{YELLOW}[USO]{RESET} py peer_no_relay.py <config.json>")


def print_startup_error(error):
    print(f"{RED}[ERRO]{RESET} falha ao iniciar o nó: {error}")


def print_liveness_event(message):
    print(f"{GREEN}[LIVENESS]{RESET} {message}")


def load_node_config(config_file_path):
    """
    Carrega o arquivo JSON de configuração do nó.

    O arquivo define os dados necessários para abrir a sessão Zenoh:
    node_id, short_id, endpoint local e endpoints dos vizinhos.
    """

    with open(config_file_path, "r", encoding="utf-8") as config_file:
        node_config = json.load(config_file)

    validate_node_config(node_config)

    return node_config


def validate_node_config(node_config):
    """
    Garante que os campos essenciais existem antes de iniciar o nó.

    Isso evita erros menos claros durante a abertura da sessão Zenoh.
    """

    missing_fields = [
        field
        for field in REQUIRED_CONFIG_FIELDS
        if field not in node_config
    ]

    if missing_fields:
        raise ValueError(
            "campos obrigatórios ausentes: "
            + ", ".join(missing_fields)
        )


def parse_json_payload(sample):
    """
    Tenta converter o payload recebido pelo Zenoh para JSON.

    Se a conversão falhar, retorna um dicionário contendo o erro e o payload
    bruto. Isso ajuda no diagnóstico de mensagens malformadas sem interromper
    a execução do nó.
    """

    raw_payload = ""

    try:
        raw_payload = sample.payload.to_string()
        return json.loads(raw_payload)

    except Exception as error:
        return {
            "_parse_error": str(error),
            "raw": raw_payload
        }


def build_zenoh_config(listen_endpoint, neighbor_endpoints, gossip_enabled):
    """
    Monta a configuração Zenoh em modo peer.

    A configuração usa:
    - um endpoint local para escuta;
    - uma lista de endpoints vizinhos para conexão;
    - multicast desabilitado;
    - gossip configurável;
    - roteamento peer em modo linkstate.

    A configuração é criada como dicionário Python e depois convertida para JSON.
    Isso reduz erros comuns ao montar JSON manualmente com f-string.
    """

    zenoh_config = {
        "mode": "peer",
        "listen": {
            "endpoints": [listen_endpoint]
        },
        "connect": {
            "endpoints": neighbor_endpoints
        },
        "scouting": {
            "multicast": {
                "enabled": False
            },
            "gossip": {
                "enabled": gossip_enabled
            }
        },
        "routing": {
            "peer": {
                "mode": "linkstate"
            }
        }
    }

    return zenoh.Config.from_json5(json.dumps(zenoh_config))


def is_own_heartbeat(heartbeat_data, node_id, short_id):
    """
    Verifica se o heartbeat recebido foi enviado pelo próprio nó.

    O nó ignora o próprio heartbeat para não se registrar como peer externo.
    """

    sender_node_id = heartbeat_data.get("node")
    sender_short_id = heartbeat_data.get("short_id")

    return (
        sender_node_id in (node_id, short_id)
        or sender_short_id in (node_id, short_id)
    )


def update_peer_status(peers_status, peers_lock, peer_node_id, peer_short_id):
    """
    Atualiza o estado de um peer quando um heartbeat válido é recebido.

    O acesso ao dicionário é protegido por lock porque ele também é lido e
    alterado pela thread de verificação de liveness.
    """

    with peers_lock:
        is_new_peer = peer_node_id not in peers_status

        was_offline = (
            peer_node_id in peers_status
            and peers_status[peer_node_id]["status"] == "OFFLINE"
        )

        peers_status[peer_node_id] = {
            "short_id": peer_short_id,
            "last_seen": time.time(),
            "status": "ONLINE"
        }

    if is_new_peer:
        print_liveness_event(f"{peer_node_id} apareceu pela primeira vez")

    elif was_offline:
        print_liveness_event(f"{peer_node_id} voltou para ONLINE")


def create_heartbeat_handler(node_id, short_id, peers_status, peers_lock):
    """
    Cria a função callback chamada sempre que um heartbeat é recebido.

    O heartbeat esperado possui, no mínimo:
    - node: identificador completo do peer;
    - short_id: identificador curto do peer.

    O callback ignora mensagens do próprio nó e atualiza o status dos peers
    externos que enviaram heartbeat.
    """

    def on_heartbeat(sample):
        heartbeat_data = parse_json_payload(sample)

        sender_node_id = heartbeat_data.get("node")
        sender_short_id = heartbeat_data.get("short_id")

        if not sender_node_id:
            return

        if is_own_heartbeat(heartbeat_data, node_id, short_id):
            return

        update_peer_status(
            peers_status,
            peers_lock,
            sender_node_id,
            sender_short_id
        )

    return on_heartbeat


def run_liveness_checker(peers_status, peers_lock):
    """
    Verifica periodicamente se algum peer parou de enviar heartbeat.

    Essa função roda em uma thread separada para que a checagem de liveness
    aconteça em paralelo com o envio contínuo dos heartbeats.
    """

    while True:
        time.sleep(LIVENESS_CHECK_INTERVAL_SECONDS)
        current_time = time.time()

        with peers_lock:
            for peer_node_id, peer_info in peers_status.items():
                if peer_info["status"] != "ONLINE":
                    continue

                elapsed_seconds = current_time - peer_info["last_seen"]

                if elapsed_seconds > PEER_TIMEOUT_SECONDS:
                    peer_info["status"] = "OFFLINE"

                    print_liveness_event(
                        f"A conexão com {peer_node_id} foi perdida "
                        f"(timeout de {int(elapsed_seconds)}s)"
                    )


def build_heartbeat_payload(node_id, short_id):
    """
    Monta o conteúdo enviado periodicamente para avisar que o nó está ativo.

    Payload publicado:
    - node: identificador completo do nó;
    - short_id: identificador curto do nó;
    - timestamp: momento em que o heartbeat foi gerado.
    """

    return {
        "node": node_id,
        "short_id": short_id,
        "timestamp": time.time()
    }


def publish_heartbeats(heartbeat_publisher, node_id, short_id):
    """
    Publica heartbeats continuamente no tópico do próprio nó.
    """

    while True:
        heartbeat_payload = build_heartbeat_payload(node_id, short_id)

        heartbeat_publisher.put(json.dumps(heartbeat_payload))

        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def run_node(node_config):
    node_id = node_config["node_id"]
    short_id = node_config["short_id"]
    listen_endpoint = node_config["listen_endpoint"]
    neighbor_endpoints = node_config["neighbor_endpoints"]
    gossip_enabled = node_config.get("gossip_enabled", False)

    heartbeat_publish_key = HEARTBEAT_TOPIC_TEMPLATE.format(node_id=node_id)

    # Guarda o último heartbeat recebido de cada peer conhecido.
    # Estrutura: peer_id -> short_id, last_seen e status ONLINE/OFFLINE.
    peers_status = {}

    # Protege o acesso ao peers_status, pois o callback Zenoh e a thread de
    # liveness podem acessar o dicionário ao mesmo tempo.
    peers_lock = threading.Lock()

    zenoh_config = build_zenoh_config(
        listen_endpoint,
        neighbor_endpoints,
        gossip_enabled
    )

    heartbeat_handler = create_heartbeat_handler(
        node_id,
        short_id,
        peers_status,
        peers_lock
    )

    print_startup_banner(node_id)

    with zenoh.open(zenoh_config) as zenoh_session:
        zenoh_session.declare_subscriber(
            ALL_HEARTBEATS_TOPIC,
            heartbeat_handler
        )

        heartbeat_publisher = zenoh_session.declare_publisher(
            heartbeat_publish_key
        )

        # Thread daemon: encerra automaticamente quando o processo principal termina.
        threading.Thread(
            target=run_liveness_checker,
            args=(peers_status, peers_lock),
            daemon=True
        ).start()

        try:
            publish_heartbeats(
                heartbeat_publisher,
                node_id,
                short_id
            )

        except KeyboardInterrupt:
            print(f"\n{YELLOW}[ENCERRANDO]{RESET} {node_id}...")


def main():
    if len(sys.argv) < 2:
        print_usage()
        return

    config_file_path = sys.argv[1]

    try:
        node_config = load_node_config(config_file_path)
        run_node(node_config)

    except Exception as error:
        print_startup_error(error)


if __name__ == "__main__":
    main()


    