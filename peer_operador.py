import time
import zenoh
import json
import os
import sys
import threading
import uuid


HEARTBEAT_INTERVAL = 5
LIVENESS_TIMEOUT = 12
CHECK_INTERVAL = 2

RETRY_INTERVAL = 3
PRINT_STATUS_INTERVAL = 6

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
GREY = "\033[90m"
RESET = "\033[0m"

def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")

def parse_payload(sample):
    try:
        return json.loads(sample.payload.to_string())
    except Exception as e:
        return {
            "_parse_error": str(e),
            "raw": sample.payload.to_string()
        }


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def print_banner(node_id):
    print("====================================")
    print(f"      {node_id} - Operador")
    print("====================================")
    print("Digite uma mensagem para enviar")
    print("Digite 'h' para esconder/mostrar logs")
    print("Digite 'exit' para sair")
    print("====================================")


def build_zenoh_config(listen_endpoint, neighbor_endpoints, gossip_enabled):
    neighbors_str = ", ".join(f'"{ep}"' for ep in neighbor_endpoints)

    return zenoh.Config.from_json5(f"""
    {{
      mode: "peer",
      listen: {{
        endpoints: ["{listen_endpoint}"]
      }},
      connect: {{
        endpoints: [{neighbors_str}]
      }},
      scouting: {{
        multicast: {{
          enabled: false
        }},
        gossip: {{
          enabled: {"true" if gossip_enabled else "false"}
        }}
      }},
      routing: {{
        peer: {{
          mode: "linkstate"
        }}
      }}
    }}
    """)


def load_outbox(outbox_file):
    if not os.path.exists(outbox_file):
        return {}

    try:
        with open(outbox_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        print(f"{RED}[OUTBOX]{RESET} erro ao carregar arquivo: {e}")

    return {}


def save_outbox(outbox_file, outbox):
    try:
        with open(outbox_file, "w", encoding="utf-8") as f:
            json.dump(outbox, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{RED}[OUTBOX]{RESET} erro ao salvar arquivo: {e}")


def build_payload(node_id, short_id, target, user_message):
    now = time.time()
    msg_id = f"{short_id}-{uuid.uuid4().hex[:12]}"

    return {
        "msg_id": msg_id,
        "origin": node_id,
        "origin_id": short_id,
        "target": target,
        "message": user_message,
        "created_at": now,
        "last_sent_at": 0,
        "attempts": 0,
        "acked": False,
        "path": [short_id]
    }


def main():
    hiden_log = False
    stop_event = threading.Event()

    ONLINE_CHECK = False;
    RELAY_CHECK = False;

    if len(sys.argv) < 2:
        print("Uso: py peer_operador.py <config.json>")
        return

    config_path = sys.argv[1]
    cfg = load_config(config_path)

    node_id = cfg["node_id"]
    short_id = cfg["short_id"]
    listen_endpoint = cfg["listen_endpoint"]
    neighbor_endpoints = cfg["neighbor_endpoints"]
    neighbor_nodes = cfg.get("neighbor_nodes", [])
    gossip_enabled = cfg["gossip_enabled"]

    target = cfg.get("target", "peer-T")
    command_key = cfg.get("command_key", "dsot/peer-T/commands")
    ack_sub_key = cfg.get("ack_sub_key", f"dsot/acks/{short_id}")
    outbox_file = cfg.get("outbox_file", f"{short_id}_outbox.json")

    heartbeat_pub_key = f"dsot/{node_id}/heartbeat"
    heartbeat_sub_key = "dsot/*/heartbeat"

    peers_status = {}
    peers_lock = threading.Lock()

    outbox_lock = threading.Lock()

    conf = build_zenoh_config(
        listen_endpoint=listen_endpoint,
        neighbor_endpoints=neighbor_endpoints,
        gossip_enabled=gossip_enabled
    )

    def enqueue_message(user_message):
        nonlocal RELAY_CHECK
        payload = build_payload(node_id, short_id, target, user_message)

        with outbox_lock:
            outbox = load_outbox(outbox_file)
            outbox[payload["msg_id"]] = payload
            save_outbox(outbox_file, outbox)

        print(f"{BLUE}[QUEUE]{RESET} mensagem adicionada: {payload['msg_id']}")
        print(f"{BLUE}[QUEUE]{RESET} conteudo: {user_message}")
        RELAY_CHECK = True

    def remove_acked_message(msg_id):
        nonlocal RELAY_CHECK
        with outbox_lock:
            outbox = load_outbox(outbox_file)

            if msg_id in outbox:
                del outbox[msg_id]
                save_outbox(outbox_file, outbox)
                print(f"{GREEN}[ACK]{RESET} mensagem confirmada e removida da fila: {msg_id}")
                RELAY_CHECK = True
            else:
                print(f"{RED}[ACK]{RESET} ACK recebido para msg_id desconhecido/removido: {msg_id}")

    def mark_attempt(msg_id):
        with outbox_lock:
            outbox = load_outbox(outbox_file)

            if msg_id not in outbox:
                return None

            outbox[msg_id]["attempts"] += 1
            outbox[msg_id]["last_sent_at"] = time.time()
            payload = dict(outbox[msg_id])

            save_outbox(outbox_file, outbox)
            return payload

    def get_pending_messages():
        with outbox_lock:
            outbox = load_outbox(outbox_file)
            return list(outbox.values())

    def input_loop():
        nonlocal hiden_log

        while not stop_event.is_set():
            try:
                raw_input_value = input("Mensagem: \n").strip()
            except EOFError:
                stop_event.set()
                break

            if raw_input_value.lower() == "exit":
                print("Encerrando operador...")
                stop_event.set()
                break

            # if raw_input_value.lower() == "h.log":
            #     hiden_log = not hiden_log
            #     print(f"{YELLOW}[INPUT]{RESET} hiden_log = {hiden_log}")
            #     continue

            if not raw_input_value:
                continue

            enqueue_message(raw_input_value)

    def on_heartbeat(sample):
        nonlocal ONLINE_CHECK

        data = parse_payload(sample)
        sender_node = data.get("node")
        sender_short_id = data.get("short_id")

        if not sender_node:
            return

        if sender_node in (node_id, short_id) or sender_short_id in (node_id, short_id):
            return

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
            print(f"{GREEN}[LIVENESS]{RESET} Novo peer detectado: {sender_node} (short_id={sender_short_id})")
            ONLINE_CHECK = True 
        if was_offline:
            print(f"{GREEN}[LIVENESS]{RESET} {sender_node} voltou para ONLINE")
            ONLINE_CHECK = True

        # if not hiden_log:
        #     print(f"[HB] {sample.key_expr}: {json.dumps(data, ensure_ascii=False)}")

    def on_ack(sample):
        data = parse_payload(sample)

        msg_id = data.get("msg_id")
        ack = data.get("ack", False)

        if not msg_id or not ack:
            print(f"{RED}[ACK]{RESET} payload invalido: {data}")
            return

        # if not hiden_log:
        #     print(f"{BLUE}[ACK-RECV]{RESET} {sample.key_expr}: {json.dumps(data, ensure_ascii=False)}")

        remove_acked_message(msg_id)

    def sender_loop(pub_command):
        while not stop_event.is_set():
            pending_messages = get_pending_messages()
            now = time.time()

            for msg in pending_messages:
                last_sent_at = msg.get("last_sent_at", 0)

                if now - last_sent_at < RETRY_INTERVAL:
                    continue

                updated_payload = mark_attempt(msg["msg_id"])
                if updated_payload is None:
                    continue

                path = updated_payload.get("path", [])
                if not isinstance(path, list):
                    path = []

                if short_id not in path:
                    path = path + [short_id]

                payload_to_send = {
                    "msg_id": updated_payload["msg_id"],
                    "origin": updated_payload["origin"],
                    "origin_id": updated_payload["origin_id"],
                    "target": updated_payload["target"],
                    "message": updated_payload["message"],
                    "created_at": updated_payload["created_at"],
                    "sent_at": time.time(),
                    "attempts": updated_payload["attempts"],
                    "path": path,
                    "relay_by": short_id,
                    "relay_node": node_id
                }

                pub_command.put(json.dumps(payload_to_send))

                # print(
                #     f"[CMD-SENT] {command_key} | "
                #     f"msg_id={payload_to_send['msg_id']} | "
                #     f"tentativa={payload_to_send['attempts']} | "
                #     f"mensagem={payload_to_send['message']}"
                # )

            time.sleep(1)

    def liveness_checker():
        nonlocal ONLINE_CHECK
        while not stop_event.is_set():
            time.sleep(CHECK_INTERVAL)
            now = time.time()

            with peers_lock:
                for peer, info in peers_status.items():
                    if info["status"] == "ONLINE":
                        elapsed = now - info["last_seen"]
                        if elapsed > LIVENESS_TIMEOUT:
                            info["status"] = "OFFLINE"
                            print(f"{GREEN}[LIVENESS]{RESET} a conexão com {peer} foi perdida (timeout de {int(elapsed)}s)")
                            if info.get("short_id") in neighbor_nodes:
                                print(f"   {RED}-> {peer} está OFFLINE{RESET} ")
                            ONLINE_CHECK = True
    def status_loop():
        nonlocal ONLINE_CHECK, RELAY_CHECK
        while not stop_event.is_set():
            time.sleep(PRINT_STATUS_INTERVAL)

            with peers_lock:
                if ONLINE_CHECK:
                    connected_STATUS = ""
                    ONLINE_CHECK = False
                    if peers_status:
                        print("\n----------- STATUS DOS PEERS -----------")
                        for peer, info in sorted(peers_status.items()):
                            if info["status"] == "ONLINE":
                                connected_STATUS = "CONNECTED"
                            else:
                                connected_STATUS = "NOT CONNECTED"
                                if info.get("short_id") in neighbor_nodes:
                                    connected_STATUS += RED + " (OFFLINE)" + RESET
                            last_seen_ago = round(time.time() - info["last_seen"], 1)
                            print(
                                f"{peer:<10} | {connected_STATUS} | "
                                f"ultimo heartbeat ha {last_seen_ago:>4}s"
                            )
                        print("----------------------------------------")

            pending_messages = get_pending_messages()
            if RELAY_CHECK:
                RELAY_CHECK = False
                print("--------------- OUTBOX -----------------")
                if not pending_messages:
                    print(f"{YELLOW}fila vazia{RESET}")
                else:
                    for msg in pending_messages:
                        age = round(time.time() - msg.get("created_at", time.time()), 1)
                        print(
                            f"{YELLOW}{msg['msg_id']}{RESET} | "
                            f"tentativas={msg.get('attempts', 0)} | "
                            f"idade={age}s | "
                            f"msg={msg.get('message', '')}"
                        )
                print("----------------------------------------\n")

    clear_terminal()
    print_banner(node_id)

    with zenoh.open(conf) as zenoh_session:
        zenoh_session.declare_subscriber(heartbeat_sub_key, on_heartbeat)
        zenoh_session.declare_subscriber(ack_sub_key, on_ack)

        pub_command = zenoh_session.declare_publisher(command_key)
        pub_heartbeat = zenoh_session.declare_publisher(heartbeat_pub_key)

        with peers_lock:
            pending_messages = get_pending_messages()
            print("--------------- OUTBOX -----------------")
            if not pending_messages:
                print(f"{YELLOW}fila vazia{RESET}")
            else:
                for msg in pending_messages:
                    age = round(time.time() - msg.get("created_at", time.time()), 1)
                    print(
                        f"{YELLOW}{msg['msg_id']}{RESET} | "
                        f"tentativas={msg.get('attempts', 0)} | "
                        f"idade={age}s | "
                        f"msg={msg.get('message', '')}"
                    )
            print("----------------------------------------\n")

        threading.Thread(target=input_loop, daemon=True).start()
        threading.Thread(target=sender_loop, args=(pub_command,), daemon=True).start()
        threading.Thread(target=liveness_checker, daemon=True).start()
        threading.Thread(target=status_loop, daemon=True).start()

        try:
            while not stop_event.is_set():
                heartbeat_payload = {
                    "node": node_id,
                    "short_id": short_id,
                    "timestamp": time.time()
                }
                pub_heartbeat.put(json.dumps(heartbeat_payload))
                time.sleep(HEARTBEAT_INTERVAL)

        except KeyboardInterrupt:
            print(f"\nEncerrando {node_id}...")
            stop_event.set()


if __name__ == "__main__":
    main()
