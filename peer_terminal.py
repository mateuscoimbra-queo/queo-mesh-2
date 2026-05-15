import zenoh
import time
import json
import os
import sys
import threading


HEARTBEAT_INTERVAL = 5
LIVENESS_TIMEOUT = 12
CHECK_INTERVAL = 2
STATUS_PRINT_INTERVAL = 1
PROCESSED_TTL = 600

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
GREY = "\033[90m"
RESET = "\033[0m"


def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")


def enable_ansi_colors():
    if os.name != "nt":
        return

    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        mode = ctypes.c_uint()

        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
    except Exception:
        pass


def color_text(text, color_code):
    return f"{color_code}{text}{RESET}"


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
    print(f"         {node_id} - Terminal")
    print("====================================")
    print("Waiting for commands...")
    print("Ctrl+C to quit")
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


def main():
    ONLINE_CHECK = False
    MESSAGE_CHECK = False

    if len(sys.argv) < 2:
        print("Uso: py peer_terminal.py <config.json>")
        return

    config_path = sys.argv[1]
    cfg = load_config(config_path)

    node_id = cfg["node_id"]
    short_id = cfg["short_id"]
    listen_endpoint = cfg["listen_endpoint"]
    neighbor_endpoints = cfg["neighbor_endpoints"]
    neighbor_nodes = cfg.get("neighbor_nodes", [])
    gossip_enabled = cfg["gossip_enabled"]

    heartbeat_pub_key = cfg.get("heartbeat_pub_key", f"dsot/{node_id}/heartbeat")
    heartbeat_sub_key = cfg.get("heartbeat_sub_key", "dsot/*/heartbeat")
    command_sub_key = cfg.get("command_sub_key", "dsot/peer-T/commands")

    peers_status = {}
    peers_lock = threading.Lock()

    processed_messages = {}
    processed_lock = threading.Lock()

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

    def send_ack(zenoh_session, msg_id, origin_id):
        ack_key = f"dsot/acks/{origin_id}"

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
            print(f"{GREEN}[LIVENESS]{RESET} {sender_node} apareceu pela primeira vez")
            ONLINE_CHECK = True
        elif was_offline:
            print(f"{GREEN}[LIVENESS]{RESET} {sender_node} voltou para ONLINE")
            ONLINE_CHECK = True

    def on_command(sample):
        nonlocal MESSAGE_CHECK

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

        with processed_lock:
            is_duplicate = msg_id in processed_messages

            if not is_duplicate:
                processed_messages[msg_id] = time.time()

            last_message_info["msg_id"] = msg_id
            last_message_info["origin_id"] = origin_id
            last_message_info["message"] = message
            last_message_info["received_at"] = time.time()
            last_message_info["duplicate"] = is_duplicate

        MESSAGE_CHECK = True

        if is_duplicate:
            print(f"mensagem repetida chegou: {msg_id}")
            send_ack(zenoh_session, msg_id, origin_id)
            return

        print(
            f"{BLUE}[CMD]{RESET} msg_id={msg_id} | "
            f"origin={origin_id} | "
            f"msg={message}"
        )
        print(f"-> {color_text(message, YELLOW)}")
        send_ack(zenoh_session, msg_id, origin_id)

    def cleanup_processed_messages():
        while True:
            time.sleep(5)
            now = time.time()

            with processed_lock:
                expired = [
                    msg_id
                    for msg_id, ts in processed_messages.items()
                    if now - ts > PROCESSED_TTL
                ]
                for msg_id in expired:
                    del processed_messages[msg_id]

    def liveness_checker():
        nonlocal ONLINE_CHECK

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
                            ONLINE_CHECK = True

    def print_status_table():
        nonlocal ONLINE_CHECK, MESSAGE_CHECK

        while True:
            time.sleep(STATUS_PRINT_INTERVAL)

            if ONLINE_CHECK:
                ONLINE_CHECK = False
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

            if MESSAGE_CHECK:
                MESSAGE_CHECK = False
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

    enable_ansi_colors()
    clear_terminal()
    print_banner(node_id)

    with zenoh.open(conf) as zenoh_session:
        zenoh_session.declare_subscriber(heartbeat_sub_key, on_heartbeat)
        zenoh_session.declare_subscriber(command_sub_key, on_command)

        pub_heartbeat = zenoh_session.declare_publisher(heartbeat_pub_key)

        with processed_lock:
            print("\n--------- STATUS DAS MENSAGENS ---------")
            print(f"{YELLOW}nenhuma mensagem recebida ainda{RESET}")
            print("----------------------------------------\n")

        threading.Thread(target=cleanup_processed_messages, daemon=True).start()
        threading.Thread(target=liveness_checker, daemon=True).start()
        threading.Thread(target=print_status_table, daemon=True).start()

        try:
            while True:
                heartbeat_payload = {
                    "node": node_id,
                    "short_id": short_id,
                    "timestamp": time.time()
                }
                pub_heartbeat.put(json.dumps(heartbeat_payload))
                time.sleep(HEARTBEAT_INTERVAL)

        except KeyboardInterrupt:
            print(f"\nEncerrando {node_id}...")


if __name__ == "__main__":
    main()