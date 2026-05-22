"""
Peer Zenoh com relay, deduplicação e retry.

Este arquivo representa um nó da malha que:
- abre uma sessão Zenoh em modo peer;
- publica heartbeats periodicamente;
- escuta heartbeats de outros peers;
- mantém o status dos peers conhecidos;
- recebe comandos;
- mantém um buffer local de relay;
- reenvia mensagens até receber ACK;
- remove mensagens já confirmadas do buffer local.
"""

import json
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field

import zenoh


HEARTBEAT_INTERVAL = 5
LIVENESS_TIMEOUT = 12
CHECK_INTERVAL = 2

HEARTBEAT_KEY_TEMPLATE = "dsot/{node_id}/heartbeat"
HEARTBEAT_SUB_KEY = "dsot/*/heartbeat"
COMMAND_KEY = "dsot/peer-T/commands"
ACK_SUB_KEY = "dsot/acks/*"

RELAY_BUFFER_MAX_SIZE = 100
RELAY_RETRY_INTERVAL = 3
MSG_CACHE_TTL = 300
ACK_CACHE_TTL = 300
CACHE_CLEANUP_INTERVAL = 5
STATUS_PRINT_INTERVAL = 1

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
GREY = "\033[90m"
RESET = "\033[0m"


@dataclass
class RuntimeState:
    """
    Agrupa os estados compartilhados entre callbacks e threads.

    O acesso aos dicionários e ao buffer é protegido por locks porque o Zenoh
    chama callbacks enquanto outras threads verificam liveness, limpam caches,
    imprimem status e reenviam mensagens.
    """

    online_check: bool = False
    relay_check: bool = False
    hiden_log: bool = False

    # Estrutura: peer_id -> short_id, last_seen e status ONLINE/OFFLINE.
    peers_status: dict = field(default_factory=dict)
    peers_lock: threading.Lock = field(default_factory=threading.Lock)

    # Estrutura: cada entrada guarda msg_id, payload original, horários e tentativas.
    relay_buffer: deque = field(
        default_factory=lambda: deque(maxlen=RELAY_BUFFER_MAX_SIZE)
    )
    relay_lock: threading.Lock = field(default_factory=threading.Lock)

    # Caches usados para deduplicar mensagens e ignorar mensagens já confirmadas.
    seen_messages: dict = field(default_factory=dict)
    acked_messages: dict = field(default_factory=dict)


def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")


def parse_payload(sample):
    """
    Converte o payload recebido pelo Zenoh para JSON.

    Se a conversão falhar, retorna um dicionário com o erro e o payload bruto,
    sem interromper a execução do nó.
    """

    try:
        return json.loads(sample.payload.to_string())
    except Exception as e:
        return {
            "_parse_error": str(e),
            "raw": sample.payload.to_string()
        }


def load_config(config_path):
    """
    Carrega o arquivo JSON de configuração do nó.
    """

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def print_banner(node_id):
    print("====================================")
    print(f"          {node_id}")
    print("====================================")
    print("Heartbeat + Relay + Dedup + Retry")
    print("Ctrl+C to quit")
    print("====================================")


def build_zenoh_config(listen_endpoint, neighbor_endpoints, gossip_enabled):
    """
    Monta a configuração Zenoh em modo peer.

    A configuração mantém o contexto original do nó:
    - endpoint local de escuta;
    - endpoints vizinhos;
    - multicast desabilitado;
    - gossip configurável;
    - roteamento peer em modo linkstate.
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


def is_own_heartbeat(data, node_id, short_id):
    """
    Verifica se o heartbeat recebido foi enviado pelo próprio nó.
    """

    sender_node = data.get("node")
    sender_short_id = data.get("short_id")

    return (
        sender_node in (node_id, short_id)
        or sender_short_id in (node_id, short_id)
    )


def update_peer_status(state, sender_node, sender_short_id):
    """
    Atualiza o status de um peer quando um heartbeat válido é recebido.
    """

    now = time.time()

    with state.peers_lock:
        is_new_peer = sender_node not in state.peers_status
        was_offline = (
            sender_node in state.peers_status
            and state.peers_status[sender_node]["status"] == "OFFLINE"
        )

        state.peers_status[sender_node] = {
            "short_id": sender_short_id,
            "last_seen": now,
            "status": "ONLINE"
        }

    if is_new_peer:
        print(f"{GREEN}[LIVENESS]{RESET} {sender_node} apareceu pela primeira vez")
        state.online_check = True
    elif was_offline:
        print(f"{GREEN}[LIVENESS]{RESET} {sender_node} voltou para ONLINE")
        state.online_check = True


def create_heartbeat_handler(state, node_id, short_id):
    """
    Cria o callback chamado quando um heartbeat é recebido.
    """

    def on_heartbeat(sample):
        data = parse_payload(sample)
        sender_node = data.get("node")

        if not sender_node:
            return

        if is_own_heartbeat(data, node_id, short_id):
            return

        update_peer_status(
            state,
            sender_node,
            data.get("short_id")
        )

        # if not hiden_log:
        #     print(f"[HB] {sample.key_expr}: {json.dumps(data, ensure_ascii=False)}")

    return on_heartbeat


def add_message_to_relay_buffer(state, msg_id, data, now):
    """
    Registra uma nova mensagem no buffer local de relay.

    Payload armazenado por entrada:
    - msg_id: identificador da mensagem;
    - payload: dados originais recebidos;
    - first_seen_at: quando a mensagem entrou no nó;
    - last_sent_at: último envio pelo relay;
    - attempts: quantidade de tentativas de relay.
    """

    state.seen_messages[msg_id] = now

    state.relay_buffer.append({
        "msg_id": msg_id,
        "payload": data,
        "first_seen_at": now,
        "last_sent_at": 0,
        "attempts": 0
    })


def create_command_handler(state):
    """
    Cria o callback chamado quando um comando é recebido.

    A mensagem só entra no buffer se tiver msg_id, ainda não tiver ACK e ainda
    não tiver sido vista por este nó.
    """

    def on_command(sample):
        data = parse_payload(sample)

        msg_id = data.get("msg_id")
        if not msg_id:
            print(f"{RED}[CMD-INVALIDO]{RESET} mensagem sem msg_id: {data}")
            return

        now = time.time()

        with state.relay_lock:
            if msg_id in state.acked_messages:
                if not state.hiden_log:
                    print(f"{GREY}[CMD-IGNORE] {msg_id} ja foi ackado {RESET}")
                return

            if msg_id in state.seen_messages:
            #     if not hiden_log:
            #         print(f"[CMD-DUP] {msg_id} duplicada ignorada")
                return

            add_message_to_relay_buffer(state, msg_id, data, now)

        state.relay_check = True
        print(
            f"{BLUE}[CMD-NEW]{RESET} msg_id={msg_id} | "
            f"origin={data.get('origin_id')} | "
            f"msg={data.get('message', '')}"
        )

    return on_command


def remove_message_from_relay_buffer(state, msg_id):
    """
    Remove do buffer local a mensagem confirmada por ACK.
    """

    for entry in list(state.relay_buffer):
        if entry["msg_id"] == msg_id:
            state.relay_buffer.remove(entry)
            return True

    return False


def create_ack_handler(state):
    """
    Cria o callback chamado quando um ACK é recebido.
    """

    def on_ack(sample):
        data = parse_payload(sample)

        msg_id = data.get("msg_id")
        ack = data.get("ack", False)

        if not msg_id or not ack:
            return

        with state.relay_lock:
            state.acked_messages[msg_id] = time.time()
            removed = remove_message_from_relay_buffer(state, msg_id)

        if removed:
            print(f"{BLUE}[ACK]{RESET} {msg_id} removida do buffer local")
            state.relay_check = True

        # print(f"{BLUE}[ACK-RECV]{RESET} {sample.key_expr}: {json.dumps(data, ensure_ascii=False)}")

    return on_ack


def build_relay_payload(entry, node_id, short_id):
    """
    Monta o payload reenviado pelo relay.

    O payload original é preservado e recebe informações do nó que está
    reenviando a mensagem.
    """

    payload = dict(entry["payload"])

    path = payload.get("path", [])
    if not isinstance(path, list):
        path = []

    if short_id not in path:
        path = path + [short_id]

    payload["path"] = path
    payload["relay_by"] = short_id
    payload["relay_node"] = node_id
    payload["relayed_at"] = time.time()

    return payload


def should_relay_entry(state, entry, msg_id, now):
    """
    Verifica se uma entrada do buffer pode ser reenviada agora.
    """

    if msg_id in state.acked_messages:
        return False

    if entry not in state.relay_buffer:
        return False

    if now - entry["last_sent_at"] < RELAY_RETRY_INTERVAL:
        return False

    return True


def update_relay_attempt(state, entry, msg_id):
    """
    Atualiza os metadados da tentativa de relay após um envio.
    """

    if entry in state.relay_buffer and msg_id not in state.acked_messages:
        entry["last_sent_at"] = time.time()
        entry["attempts"] += 1


def relay_sender_loop(state, pub_command, node_id, short_id):
    """
    Reenvia mensagens pendentes do buffer enquanto elas não recebem ACK.
    """

    while True:
        now = time.time()

        with state.relay_lock:
            items = list(state.relay_buffer)

        for entry in items:
            msg_id = entry["msg_id"]

            with state.relay_lock:
                if not should_relay_entry(state, entry, msg_id, now):
                    continue

            payload = build_relay_payload(entry, node_id, short_id)
            pub_command.put(json.dumps(payload))

            with state.relay_lock:
                update_relay_attempt(state, entry, msg_id)

            # print(f"[RELAY] msg_id={msg_id} | tentativa={attempts}")

            # if attempts >= 15:
            #     print(f"[RELAY-FAIL] msg_id={msg_id} | tentativas={attempts}")
            #     with relay_lock:
            #         if msg_id in relay_buffer:
            #             del relay_buffer[msg_id]

            time.sleep(1)


def liveness_checker(state, neighbor_nodes):
    """
    Marca peers como OFFLINE quando param de enviar heartbeat.
    """

    while True:
        time.sleep(CHECK_INTERVAL)
        now = time.time()

        with state.peers_lock:
            for peer, info in state.peers_status.items():
                if info["status"] == "ONLINE":
                    elapsed = now - info["last_seen"]
                    if elapsed > LIVENESS_TIMEOUT:
                        info["status"] = "OFFLINE"
                        print(f"{GREEN}[LIVENESS]{RESET} A conexão com {peer} foi perdida (timeout de {int(elapsed)}s)")
                        if info.get("short_id") in neighbor_nodes:
                            print(f"   {RED}-> {peer} ficou OFFLINE {RESET} ")
                        state.online_check = True


def cleanup_caches(state):
    """
    Remove registros antigos dos caches de deduplicação e ACK.
    """

    while True:
        time.sleep(CACHE_CLEANUP_INTERVAL)
        now = time.time()

        with state.relay_lock:
            seen_to_remove = [
                msg_id
                for msg_id, ts in state.seen_messages.items()
                if now - ts > MSG_CACHE_TTL
            ]
            for msg_id in seen_to_remove:
                del state.seen_messages[msg_id]

            ack_to_remove = [
                msg_id
                for msg_id, ts in state.acked_messages.items()
                if now - ts > ACK_CACHE_TTL
            ]
            for msg_id in ack_to_remove:
                del state.acked_messages[msg_id]


def print_peers_status(state, neighbor_nodes):
    """
    Imprime a tabela de status dos peers conhecidos.
    """

    connect_STATUS = ""

    with state.peers_lock:
        if state.peers_status:
            print("\n----------- STATUS DOS PEERS -----------")
            for peer, info in sorted(state.peers_status.items()):
                if info["status"] == "ONLINE":
                    connect_STATUS = "CONNECTED"
                else:
                    connect_STATUS = "NOT CONNECTED"
                    if info.get("short_id") in neighbor_nodes:
                        connect_STATUS = f"NOT CONNECTED {RED}(OFFLINE){RESET}"

                last_seen_ago = round(time.time() - info["last_seen"], 1)
                print(
                    f"{peer:<10} | {connect_STATUS} | "
                    f"ultimo heartbeat ha {last_seen_ago:>4}s"
                )
        print("----------------------------------------")


def print_relay_buffer(state):
    """
    Imprime o estado atual do buffer de relay.
    """

    with state.relay_lock:
        print("\n----------- BUFFER DE RELAY ------------")
        if not state.relay_buffer:
            print(f"{YELLOW}buffer vazio{RESET}")
        else:
            for entry in state.relay_buffer:
                msg_id = entry["msg_id"]
                age = round(time.time() - entry["first_seen_at"], 1)
                print(
                    f"{YELLOW} msg={entry['payload'].get('message', '')} {RESET}| {msg_id} | tentativas={entry['attempts']} | "
                    f"idade={age}s"
                )
        print("----------------------------------------\n")


def print_status_table(state, neighbor_nodes):
    """
    Imprime status somente quando há mudança de liveness ou relay.
    """

    while True:
        time.sleep(STATUS_PRINT_INTERVAL)

        if state.online_check == True:
            state.online_check = False
            print_peers_status(state, neighbor_nodes)

        if state.relay_check == True:
            state.relay_check = False
            print_relay_buffer(state)


def build_heartbeat_payload(node_id, short_id):
    """
    Monta o heartbeat publicado periodicamente pelo nó.
    """

    return {
        "node": node_id,
        "short_id": short_id,
        "timestamp": time.time()
    }


def publish_heartbeats(pub_heartbeat, node_id, short_id):
    """
    Publica heartbeats continuamente.
    """

    while True:
        heartbeat_payload = build_heartbeat_payload(node_id, short_id)
        pub_heartbeat.put(json.dumps(heartbeat_payload))
        time.sleep(HEARTBEAT_INTERVAL)


def start_background_threads(state, pub_command, node_id, short_id, neighbor_nodes):
    """
    Inicia as threads auxiliares do nó.
    """

    # threading.Thread(target=input_loop, daemon=True).start()
    threading.Thread(
        target=liveness_checker,
        args=(state, neighbor_nodes),
        daemon=True
    ).start()
    threading.Thread(
        target=cleanup_caches,
        args=(state,),
        daemon=True
    ).start()
    threading.Thread(
        target=print_status_table,
        args=(state, neighbor_nodes),
        daemon=True
    ).start()
    threading.Thread(
        target=relay_sender_loop,
        args=(state, pub_command, node_id, short_id),
        daemon=True
    ).start()


def run_node(cfg):
    """
    Prepara o estado do nó, registra subscribers/publishers e inicia a execução.
    """

    node_id = cfg["node_id"]
    short_id = cfg["short_id"]
    listen_endpoint = cfg["listen_endpoint"]
    neighbor_endpoints = cfg["neighbor_endpoints"]
    neighbor_nodes = cfg.get("neighbor_nodes", [])
    gossip_enabled = cfg["gossip_enabled"]

    heartbeat_pub_key = HEARTBEAT_KEY_TEMPLATE.format(node_id=node_id)

    state = RuntimeState()

    conf = build_zenoh_config(
        listen_endpoint=listen_endpoint,
        neighbor_endpoints=neighbor_endpoints,
        gossip_enabled=gossip_enabled
    )

    clear_terminal()
    print_banner(node_id)

    with zenoh.open(conf) as zenoh_session:
        zenoh_session.declare_subscriber(
            HEARTBEAT_SUB_KEY,
            create_heartbeat_handler(state, node_id, short_id)
        )
        zenoh_session.declare_subscriber(
            COMMAND_KEY,
            create_command_handler(state)
        )
        zenoh_session.declare_subscriber(
            ACK_SUB_KEY,
            create_ack_handler(state)
        )

        pub_heartbeat = zenoh_session.declare_publisher(heartbeat_pub_key)
        pub_command = zenoh_session.declare_publisher(COMMAND_KEY)

        start_background_threads(
            state,
            pub_command,
            node_id,
            short_id,
            neighbor_nodes
        )

        print_relay_buffer(state)

        try:
            publish_heartbeats(pub_heartbeat, node_id, short_id)

        except KeyboardInterrupt:
            print(f"\nEncerrando {node_id}...")


def main():
    if len(sys.argv) < 2:
        print("Uso: py peer_generic.py <config.json>")
        return

    config_path = sys.argv[1]
    cfg = load_config(config_path)

    run_node(cfg)


if __name__ == "__main__":
    main()