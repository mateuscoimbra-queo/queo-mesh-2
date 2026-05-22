"""
Peer Zenoh terminal.

Este arquivo representa o nó terminal da malha que:
- abre uma sessão Zenoh em modo peer;
- publica heartbeats periodicamente;
- escuta heartbeats de outros peers;
- mantém o status dos peers conhecidos;
- recebe comandos;
- identifica mensagens duplicadas;
- envia ACK para a origem da mensagem;
- mantém um cache temporário de mensagens processadas.
"""

import json
import os
import sys
import threading
import time

import zenoh


HEARTBEAT_INTERVAL = 5
LIVENESS_TIMEOUT = 12
CHECK_INTERVAL = 2
STATUS_PRINT_INTERVAL = 1
PROCESSED_TTL = 600
PROCESSED_CLEANUP_INTERVAL = 5

HEARTBEAT_TOPIC_TEMPLATE = "dsot/{node_id}/heartbeat"
DEFAULT_HEARTBEAT_SUB_KEY = "dsot/*/heartbeat"
DEFAULT_COMMAND_SUB_KEY = "dsot/peer-T/commands"
ACK_TOPIC_TEMPLATE = "dsot/acks/{origin_id}"

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
GREY = "\033[90m"
RESET = "\033[0m"


def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")


def enable_ansi_colors():
    """
    Habilita suporte a cores ANSI no terminal do Windows.

    Em sistemas que não são Windows, nenhuma ação é necessária.
    """

    if os.name != "nt":
        return

    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        mode = ctypes.c_uint()

        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(
                handle,
                mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING
            )
    except Exception:
        pass


def color_text(text, color_code):
    return f"{color_code}{text}{RESET}"


def parse_payload(sample):
    """
    Converte o payload recebido pelo Zenoh para JSON.

    Se a conversão falhar, retorna um dicionário com o erro e o payload bruto,
    sem interromper a execução do terminal.
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
    Carrega o arquivo JSON de configuração do terminal.
    """

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def print_banner(node_id):
    print("====================================")
    print(f"         {node_id} - Terminal")
    print("====================================")
    print("Waiting for commands...")
    print("Ctrl+C to quit")
    print("====================================")


def build_zenoh_config(listen_endpoint, neighbor_endpoints, gossip_enabled):
    """
    Monta a configuração Zenoh em modo peer.

    A configuração mantém o comportamento original:
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


def send_ack(zenoh_session, node_id, short_id, msg_id, origin_id):
    """
    Envia ACK para a origem da mensagem recebida.
    """

    ack_key = ACK_TOPIC_TEMPLATE.format(origin_id=origin_id)

    ack_payload = {
        "ack": True,
        "msg_id": msg_id,
        "from": node_id,
        "from_id": short_id,
        "to": origin_id,
        "timestamp": time.time()
    }

    zenoh_session.put(ack_key, json.dumps(ack_payload))
    print(f"{BLUE}[ACK-SENT]{RESET} {ack_key}: {json.dumps(ack_payload, ensure_ascii=False)}")


def is_own_heartbeat(heartbeat_data, node_id, short_id):
    """
    Verifica se o heartbeat recebido foi enviado pelo próprio terminal.
    """

    sender_node = heartbeat_data.get("node")
    sender_short_id = heartbeat_data.get("short_id")

    return (
        sender_node in (node_id, short_id)
        or sender_short_id in (node_id, short_id)
    )


def update_peer_status(
    peers_status,
    peers_lock,
    runtime_flags,
    sender_node,
    sender_short_id
):
    """
    Atualiza o estado de um peer quando um heartbeat válido é recebido.

    Estrutura usada em peers_status:
    peer_id -> short_id, last_seen e status ONLINE/OFFLINE.
    """

    now = time.time()

    with peers_lock:
        is_new_peer = sender_node not in peers_status
        was_offline = (
            sender_node in peers_status
            and peers_status[sender_node]["status"] == "OFFLINE"
        )

        peers_status[sender_node] = {
            "short_id": sender_short_id,
            "last_seen": now,
            "status": "ONLINE"
        }

    if is_new_peer:
        print(f"{GREEN}[LIVENESS]{RESET} {sender_node} apareceu pela primeira vez")
        runtime_flags["ONLINE_CHECK"] = True
    elif was_offline:
        print(f"{GREEN}[LIVENESS]{RESET} {sender_node} voltou para ONLINE")
        runtime_flags["ONLINE_CHECK"] = True


def create_heartbeat_handler(
    node_id,
    short_id,
    peers_status,
    peers_lock,
    runtime_flags
):
    """
    Cria o callback chamado sempre que um heartbeat é recebido.
    """

    def on_heartbeat(sample):
        data = parse_payload(sample)
        sender_node = data.get("node")
        sender_short_id = data.get("short_id")

        if not sender_node:
            return

        if is_own_heartbeat(data, node_id, short_id):
            return

        update_peer_status(
            peers_status,
            peers_lock,
            runtime_flags,
            sender_node,
            sender_short_id
        )

    return on_heartbeat


def update_last_message_info(
    last_message_info,
    msg_id,
    origin_id,
    message,
    is_duplicate
):
    """
    Atualiza os dados da última mensagem recebida.

    Estrutura usada em last_message_info:
    msg_id, origin_id, message, received_at e duplicate.
    """

    last_message_info["msg_id"] = msg_id
    last_message_info["origin_id"] = origin_id
    last_message_info["message"] = message
    last_message_info["received_at"] = time.time()
    last_message_info["duplicate"] = is_duplicate


def register_processed_message(
    processed_messages,
    processed_lock,
    last_message_info,
    msg_id,
    origin_id,
    message
):
    """
    Registra a mensagem no cache de processadas.

    Se o msg_id já existir no cache, a mensagem é tratada como duplicada.
    """

    with processed_lock:
        is_duplicate = msg_id in processed_messages

        if not is_duplicate:
            processed_messages[msg_id] = time.time()

        update_last_message_info(
            last_message_info,
            msg_id,
            origin_id,
            message,
            is_duplicate
        )

    return is_duplicate


def create_command_handler(
    zenoh_session,
    node_id,
    short_id,
    processed_messages,
    processed_lock,
    last_message_info,
    runtime_flags
):
    """
    Cria o callback chamado quando um comando é recebido.

    O comando precisa conter:
    - msg_id: identificador da mensagem;
    - origin_id: identificador curto da origem;
    - message: conteúdo enviado.
    """

    def on_command(sample):
        data = parse_payload(sample)

        msg_id = data.get("msg_id")
        origin_id = data.get("origin_id")
        message = data.get("message", "")

        if not msg_id:
            print(f"{RED}[CMD-INVALIDO]{RESET} mensagem sem msg_id: {data}")
            return

        if not origin_id:
            print(f"{RED}[CMD-INVALIDO]{RESET} mensagem sem origin_id: {data}")
            return

        is_duplicate = register_processed_message(
            processed_messages,
            processed_lock,
            last_message_info,
            msg_id,
            origin_id,
            message
        )

        runtime_flags["MESSAGE_CHECK"] = True

        if is_duplicate:
            print(f"mensagem repetida chegou: {msg_id}")
            send_ack(zenoh_session, node_id, short_id, msg_id, origin_id)
            return

        print(
            f"{BLUE}[CMD]{RESET} msg_id={msg_id} | "
            f"origin={origin_id} | "
            f"msg={message}"
        )
        print(f"-> {color_text(message, YELLOW)}")
        send_ack(zenoh_session, node_id, short_id, msg_id, origin_id)

    return on_command


def cleanup_processed_messages(processed_messages, processed_lock):
    """
    Remove mensagens antigas do cache de processadas.
    """

    while True:
        time.sleep(PROCESSED_CLEANUP_INTERVAL)
        now = time.time()

        with processed_lock:
            expired = [
                msg_id
                for msg_id, ts in processed_messages.items()
                if now - ts > PROCESSED_TTL
            ]

            for msg_id in expired:
                del processed_messages[msg_id]


def liveness_checker(
    peers_status,
    peers_lock,
    neighbor_nodes,
    runtime_flags
):
    """
    Marca peers como OFFLINE quando deixam de enviar heartbeat.
    """

    while True:
        time.sleep(CHECK_INTERVAL)
        now = time.time()

        with peers_lock:
            for peer, info in peers_status.items():
                if info["status"] == "ONLINE":
                    elapsed = now - info["last_seen"]

                    if elapsed > LIVENESS_TIMEOUT:
                        info["status"] = "OFFLINE"
                        print(f"{GREEN}[LIVENESS]{RESET} A conexão com {peer} foi perdida (timeout de {int(elapsed)}s)")

                        if info.get("short_id") in neighbor_nodes:
                            print(f"   {RED}-> {peer} ficou OFFLINE{RESET}")

                        runtime_flags["ONLINE_CHECK"] = True


def print_peers_status_if_needed(
    peers_status,
    peers_lock,
    neighbor_nodes,
    runtime_flags
):
    """
    Imprime a tabela de status dos peers somente quando há alteração.
    """

    if ONLINE_CHECK := runtime_flags["ONLINE_CHECK"]:
        runtime_flags["ONLINE_CHECK"] = False

        with peers_lock:
            if peers_status:
                print("\n----------- STATUS DOS PEERS -----------")

                for peer, info in sorted(peers_status.items()):
                    if info["status"] == "ONLINE":
                        connect_status = "CONNECTED"
                    else:
                        connect_status = "NOT CONNECTED"

                        if info.get("short_id") in neighbor_nodes:
                            connect_status = f"NOT CONNECTED {RED}(OFFLINE){RESET}"

                    last_seen_ago = round(time.time() - info["last_seen"], 1)

                    print(
                        f"{peer:<10} | {connect_status} | "
                        f"ultimo heartbeat ha {last_seen_ago:>4}s"
                    )

                print("----------------------------------------")


def print_initial_message_status():
    """
    Imprime o status inicial das mensagens processadas.
    """

    print("\n--------- STATUS DAS MENSAGENS ---------")
    print(f"{YELLOW}nenhuma mensagem recebida ainda{RESET}")
    print("----------------------------------------\n")


def print_message_status_if_needed(
    processed_messages,
    processed_lock,
    last_message_info,
    runtime_flags
):
    """
    Imprime o status das mensagens somente quando uma mensagem é recebida.
    """

    if runtime_flags["MESSAGE_CHECK"]:
        runtime_flags["MESSAGE_CHECK"] = False

        with processed_lock:
            total_msgs = len(processed_messages)

            print("\n--------- STATUS DAS MENSAGENS ---------")
            print(f"processadas no cache: {total_msgs}")

            if last_message_info["msg_id"] is None:
                print(f"{YELLOW}nenhuma mensagem recebida{RESET}")
            else:
                age = round(time.time() - last_message_info["received_at"], 1)
                tipo = "duplicada" if last_message_info["duplicate"] else "nova"

                print(
                    f"{YELLOW}{last_message_info['msg_id']}{RESET} | "
                    f"origem={last_message_info['origin_id']} | "
                    f"tipo={tipo} | "
                    f"idade={age}s | "
                    f"msg={last_message_info['message']}"
                )

            print("----------------------------------------\n")


def print_status_table(
    peers_status,
    peers_lock,
    neighbor_nodes,
    processed_messages,
    processed_lock,
    last_message_info,
    runtime_flags
):
    """
    Imprime status de peers e mensagens quando há alterações relevantes.
    """

    while True:
        time.sleep(STATUS_PRINT_INTERVAL)

        print_peers_status_if_needed(
            peers_status,
            peers_lock,
            neighbor_nodes,
            runtime_flags
        )

        print_message_status_if_needed(
            processed_messages,
            processed_lock,
            last_message_info,
            runtime_flags
        )


def build_heartbeat_payload(node_id, short_id):
    """
    Monta o heartbeat publicado periodicamente pelo terminal.
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


def start_background_threads(
    peers_status,
    peers_lock,
    neighbor_nodes,
    processed_messages,
    processed_lock,
    last_message_info,
    runtime_flags
):
    """
    Inicia as threads auxiliares do terminal.
    """

    threading.Thread(
        target=cleanup_processed_messages,
        args=(processed_messages, processed_lock),
        daemon=True
    ).start()

    threading.Thread(
        target=liveness_checker,
        args=(peers_status, peers_lock, neighbor_nodes, runtime_flags),
        daemon=True
    ).start()

    threading.Thread(
        target=print_status_table,
        args=(
            peers_status,
            peers_lock,
            neighbor_nodes,
            processed_messages,
            processed_lock,
            last_message_info,
            runtime_flags
        ),
        daemon=True
    ).start()


def run_node(cfg):
    """
    Prepara o terminal, registra subscribers/publishers e inicia a execução.
    """

    node_id = cfg["node_id"]
    short_id = cfg["short_id"]
    listen_endpoint = cfg["listen_endpoint"]
    neighbor_endpoints = cfg["neighbor_endpoints"]
    neighbor_nodes = cfg.get("neighbor_nodes", [])
    gossip_enabled = cfg["gossip_enabled"]

    heartbeat_pub_key = cfg.get(
        "heartbeat_pub_key",
        HEARTBEAT_TOPIC_TEMPLATE.format(node_id=node_id)
    )
    heartbeat_sub_key = cfg.get("heartbeat_sub_key", DEFAULT_HEARTBEAT_SUB_KEY)
    command_sub_key = cfg.get("command_sub_key", DEFAULT_COMMAND_SUB_KEY)

    # Flags simples usadas pelas threads para decidir quando imprimir status.
    runtime_flags = {
        "ONLINE_CHECK": False,
        "MESSAGE_CHECK": False
    }

    # Guarda o último heartbeat recebido de cada peer conhecido.
    # Estrutura: peer_id -> short_id, last_seen e status ONLINE/OFFLINE.
    peers_status = {}

    # Protege o acesso ao peers_status, usado pelo callback Zenoh e pelo liveness.
    peers_lock = threading.Lock()

    # Cache de mensagens já processadas para detectar duplicatas.
    # Estrutura: msg_id -> timestamp em que foi processada.
    processed_messages = {}
    processed_lock = threading.Lock()

    # Guarda dados da última mensagem recebida para impressão no status.
    last_message_info = {
        "msg_id": None,
        "origin_id": None,
        "message": None,
        "received_at": None,
        "duplicate": False
    }

    conf = build_zenoh_config(
        listen_endpoint=listen_endpoint,
        neighbor_endpoints=neighbor_endpoints,
        gossip_enabled=gossip_enabled
    )

    enable_ansi_colors()
    clear_terminal()
    print_banner(node_id)

    with zenoh.open(conf) as zenoh_session:
        zenoh_session.declare_subscriber(
            heartbeat_sub_key,
            create_heartbeat_handler(
                node_id,
                short_id,
                peers_status,
                peers_lock,
                runtime_flags
            )
        )

        zenoh_session.declare_subscriber(
            command_sub_key,
            create_command_handler(
                zenoh_session,
                node_id,
                short_id,
                processed_messages,
                processed_lock,
                last_message_info,
                runtime_flags
            )
        )

        pub_heartbeat = zenoh_session.declare_publisher(heartbeat_pub_key)

        with processed_lock:
            print_initial_message_status()

        start_background_threads(
            peers_status,
            peers_lock,
            neighbor_nodes,
            processed_messages,
            processed_lock,
            last_message_info,
            runtime_flags
        )

        try:
            publish_heartbeats(pub_heartbeat, node_id, short_id)

        except KeyboardInterrupt:
            print(f"\nEncerrando {node_id}...")


def main():
    if len(sys.argv) < 2:
        print("Uso: py peer_terminal.py <config.json>")
        return

    config_path = sys.argv[1]
    cfg = load_config(config_path)

    run_node(cfg)


if __name__ == "__main__":
    main()