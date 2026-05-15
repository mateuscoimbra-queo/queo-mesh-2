import zenoh
import time
import json
import os
import sys
import threading
from collections import deque


HEARTBEAT_INTERVAL = 5
LIVENESS_TIMEOUT = 12
CHECK_INTERVAL = 2

COMMAND_KEY = "dsot/peer-T/commands"
ACK_SUB_KEY = "dsot/acks/*"

RELAY_RETRY_INTERVAL = 3
MSG_CACHE_TTL = 300
ACK_CACHE_TTL = 300
STATUS_PRINT_INTERVAL = 1

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
    print(f"          {node_id}")
    print("====================================")
    print("Heartbeat + Relay + Dedup + Retry")
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
    RELAY_CHECK = False
    hiden_log = False

    if len(sys.argv) < 2:
        print("Uso: py peer_generic.py <config.json>")
        return

    config_path = sys.argv[1]
    cfg = load_config(config_path)

    node_id = cfg["node_id"]
    short_id = cfg["short_id"]
    listen_endpoint = cfg["listen_endpoint"]
    neighbor_endpoints = cfg["neighbor_endpoints"]
    neighbor_nodes = cfg.get("neighbor_nodes", [])
    gossip_enabled = cfg["gossip_enabled"]

    heartbeat_pub_key = f"dsot/{node_id}/heartbeat"
    heartbeat_sub_key = "dsot/*/heartbeat"

    peers_status = {}
    peers_lock = threading.Lock()

    relay_buffer = deque(maxlen=100)
    relay_lock = threading.Lock()

    seen_messages = {}
    acked_messages = {}

    conf = build_zenoh_config(
        listen_endpoint=listen_endpoint,
        neighbor_endpoints=neighbor_endpoints,
        gossip_enabled=gossip_enabled
    )

    # def input_loop():
    #     nonlocal hiden_log

    #     while True:
    #         comando = input().strip().lower()

    #         if comando == "h":
    #             hiden_log = not hiden_log
    #             print(f"[INPUT] hiden_log = {hiden_log}")

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
            
        # if not hiden_log:
        #     print(f"[HB] {sample.key_expr}: {json.dumps(data, ensure_ascii=False)}")

    def on_command(sample):
        nonlocal RELAY_CHECK

        data = parse_payload(sample)

        msg_id = data.get("msg_id")
        if not msg_id:
            print(f"{RED}[CMD-INVALIDO]{RESET} mensagem sem msg_id: {data}")
            return

        now = time.time()

        with relay_lock:
            if msg_id in acked_messages:
                if not hiden_log:
                    print(f"{GREY}[CMD-IGNORE] {msg_id} ja foi ackado {RESET}")
                return

            if msg_id in seen_messages:
            #     if not hiden_log:
            #         print(f"[CMD-DUP] {msg_id} duplicada ignorada")
                return

            seen_messages[msg_id] = now

            relay_buffer.append({
                "msg_id": msg_id,
                "payload": data,
                "first_seen_at": now,
                "last_sent_at": 0,
                "attempts": 0
            })

        RELAY_CHECK = True
        print(
            f"{BLUE}[CMD-NEW]{RESET} msg_id={msg_id} | "
            f"origin={data.get('origin_id')} | "
            f"msg={data.get('message', '')}"
        )

    def on_ack(sample):
        nonlocal RELAY_CHECK
        data = parse_payload(sample)

        msg_id = data.get("msg_id")
        ack = data.get("ack", False)

        if not msg_id or not ack:
            return

        removed = False

        with relay_lock:
            acked_messages[msg_id] = time.time()

            for entry in list(relay_buffer):
                if entry["msg_id"] == msg_id:
                    relay_buffer.remove(entry)
                    removed = True
                    break

        if removed:
            print(f"{BLUE}[ACK]{RESET} {msg_id} removida do buffer local")
            RELAY_CHECK = True

        # print(f"{BLUE}[ACK-RECV]{RESET} {sample.key_expr}: {json.dumps(data, ensure_ascii=False)}")

    def relay_sender_loop(pub_command):
        while True:
            now = time.time()

            with relay_lock:
                items = list(relay_buffer)

            for entry in items:
                msg_id = entry["msg_id"]

                with relay_lock:
                    if msg_id in acked_messages:
                        continue

                    if entry not in relay_buffer:
                        continue

                    if now - entry["last_sent_at"] < RELAY_RETRY_INTERVAL:
                        continue

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

                pub_command.put(json.dumps(payload))

                with relay_lock:
                    if entry in relay_buffer and msg_id not in acked_messages:
                        entry["last_sent_at"] = time.time()
                        entry["attempts"] += 1



                # print(f"[RELAY] msg_id={msg_id} | tentativa={attempts}")

                # if attempts >= 15:
                #     print(f"[RELAY-FAIL] msg_id={msg_id} | tentativas={attempts}")
                #     with relay_lock:
                #         if msg_id in relay_buffer:
                #             del relay_buffer[msg_id]

                time.sleep(1)

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
                                print(f"   {RED}-> {peer} ficou OFFLINE {RESET} ")
                            ONLINE_CHECK = True
    def cleanup_caches():
        while True:
            time.sleep(5)
            now = time.time()

            with relay_lock:
                seen_to_remove = [
                    msg_id
                    for msg_id, ts in seen_messages.items()
                    if now - ts > MSG_CACHE_TTL
                ]
                for msg_id in seen_to_remove:
                    del seen_messages[msg_id]

                ack_to_remove = [
                    msg_id
                    for msg_id, ts in acked_messages.items()
                    if now - ts > ACK_CACHE_TTL
                ]
                for msg_id in ack_to_remove:
                    del acked_messages[msg_id]

    def print_status_table():
        nonlocal ONLINE_CHECK, RELAY_CHECK

        while True:
            time.sleep(STATUS_PRINT_INTERVAL)
            if ONLINE_CHECK == True:
                connect_STATUS = ""
                ONLINE_CHECK = False
                with peers_lock:
                    if peers_status:
                        print("\n----------- STATUS DOS PEERS -----------")
                        for peer, info in sorted(peers_status.items()):
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
            if RELAY_CHECK == True:
                RELAY_CHECK = False
                with relay_lock:
                    print("\n----------- BUFFER DE RELAY ------------")
                    if not relay_buffer:
                        print(f"{YELLOW}buffer vazio{RESET}")
                    else:
                        for entry in relay_buffer:
                            msg_id = entry["msg_id"]
                            age = round(time.time() - entry["first_seen_at"], 1)
                            print(
                                f"{YELLOW} msg={entry['payload'].get('message', '')} {RESET}| {msg_id} | tentativas={entry['attempts']} | "
                                f"idade={age}s"
                            )
                    print("----------------------------------------\n")

    clear_terminal()
    print_banner(node_id)

    with zenoh.open(conf) as zenoh_session:
        zenoh_session.declare_subscriber(heartbeat_sub_key, on_heartbeat)
        zenoh_session.declare_subscriber(COMMAND_KEY, on_command)
        zenoh_session.declare_subscriber(ACK_SUB_KEY, on_ack)

        pub_heartbeat = zenoh_session.declare_publisher(heartbeat_pub_key)
        pub_command = zenoh_session.declare_publisher(COMMAND_KEY)

        # threading.Thread(target=input_loop, daemon=True).start()
        threading.Thread(target=liveness_checker, daemon=True).start()
        threading.Thread(target=cleanup_caches, daemon=True).start()
        threading.Thread(target=print_status_table, daemon=True).start()
        threading.Thread(target=relay_sender_loop, args=(pub_command,), daemon=True).start()

        with relay_lock:
            print("\n----------- BUFFER DE RELAY ------------")
            if not relay_buffer:
                print(f"{YELLOW}buffer vazio{RESET}")
            else:
                for entry in relay_buffer:
                    msg_id = entry["msg_id"]
                    age = round(time.time() - entry["first_seen_at"], 1)
                    print(
                        f"{YELLOW} msg={entry['payload'].get('message', '')} {RESET}| {msg_id} | tentativas={entry['attempts']} | "
                        f"idade={age}s"
                    )
            print("----------------------------------------\n")

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
