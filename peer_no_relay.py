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

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
GREY = "\033[90m"
RESET = "\033[0m"

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

def parse_payload(sample):
    try:
        return json.loads(sample.payload.to_string())
    except Exception as e:
        return {
            "_parse_error": str(e), 
            "raw": sample.payload.to_string()
            }

def load_config():
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)
    
def print_banner(node_id):
    clear_terminal()
    print("====================================")
    print(f"          {node_id}")
    print("====================================")
    print("Zenoh Mesh Node - SEM RELAY")
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
        multicast: {{ enabled: false }},
        gossip: {{ enabled: {"true" if gossip_enabled else "false"} }}
      }},
      routing: {{
        peer: {{
          mode: "linkstate"
        }}
      }}
    }}
    """)

def main():
    if len(sys.argv) < 2:
        print("Usage: python peer_no_relay.py <config_file>")
        return
    
    config_path = sys.argv[1]
    cfg = load_config(config_path)

    node_id = cfg["node_id"]
    short_id = cfg["short_id"]
    listen_endpoint = cfg["listen_endpoint"]
    neighbor_endpoints = cfg["neighbor_endpoints"]
    gossip_enabled = cfg.get["gossip_enabled"]

    heartbeat_pub_key = f"dsot/{node_id}/heartbeat"
    heartbeat_sub_key = "dsot/*/heartbeat"

    peers_status = {}
    peers_lock = threading.Lock()

    conf = build_zenoh_config(
        listen_endpoint, 
        neighbor_endpoints, 
        gossip_enabled
    )

    def on_heartbeat(sample):
        data = parse_payload(sample)
        sender_node = data.get("node")
        sender_short_id = data.get("short_id")

        if not sender_node:
            return

        if sender_node in (node_id, short_id) or sender_short_id in (node_id, short_id):
            return

        with peers_lock:
            peers_status[sender_node] = {
                "short_id": sender_short_id,
                "last_seen": time.time(),
                "status": "ONLINE"
            }

        def liveness_checker():
            while True:
                time.sleep(CHECK_INTERVAL)
                now = time.time()

                with peers_lock:
                    for peer, info in peers_status.items():
                        if info["status"] == "ONLINE":
                            elapsed = now - info["last_seen"]

                            if elapsed > LIVENESS_TIMEOUT:
                                info["status"] = "OFFLINE"
                                print(f"{RED}[LIVENESS]{RESET} {peer} ficou OFFLINE")
        clear_terminal()
        print_banner(node_id)

        with zenoh.open(conf) as zenoh_session:
            zenoh_session.declare_subscriber(heartbeat_sub_key, on_heartbeat)
            pub_heartbeat = zenoh_session.declare_publisher(heartbeat_pub_key)

            threading.Thread(target=liveness_checker, daemon=True).start()

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

