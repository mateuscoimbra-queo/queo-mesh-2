"""
Peer Zenoh operador sem relay manual.

Este arquivo representa o nó operador da malha que:
- abre uma sessão Zenoh em modo peer;
- publica heartbeats periodicamente;
- escuta heartbeats de outros peers;
- mantém o status dos peers conhecidos;
- recebe mensagens digitadas pelo usuário;
- salva mensagens pendentes em uma outbox local;
- publica mensagens no tópico de comando;
- reenvia mensagens pendentes até receber ACK;
- remove da outbox mensagens confirmadas.

A aplicação não faz roteamento manual, não escolhe próximo peer,
não encaminha mensagens recebidas e não controla hops/path.
A entrega entre peers fica sob responsabilidade da infraestrutura Zenoh.
"""

import json
import os
import sys
import threading
import time
import uuid

import zenoh


HEARTBEAT_INTERVAL = 5
LIVENESS_TIMEOUT = 12
CHECK_INTERVAL = 2

RETRY_INTERVAL = 3
PRINT_STATUS_INTERVAL = 6

HEARTBEAT_TOPIC_TEMPLATE = "dsot/{node_id}/heartbeat"
HEARTBEAT_SUB_KEY = "dsot/*/heartbeat"

DEFAULT_TARGET = "peer-T"
DEFAULT_COMMAND_KEY = "dsot/peer-T/commands"
ACK_SUB_KEY_TEMPLATE = "dsot/acks/{short_id}"
OUTBOX_FILE_TEMPLATE = "{short_id}_outbox.json"

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"


def clear_terminal():
    os.system("cls" if os.name == "nt" else "clear")


def parse_payload(sample):
    """
    Converte o payload recebido pelo Zenoh para JSON.

    Se a conversão falhar, retorna um dicionário com o erro e o payload bruto,
    sem interromper a execução do operador.
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
    Carrega o arquivo JSON de configuração do operador.
    """

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def print_banner(node_id):
    print("====================================")
    print(f"      {node_id} - Operador")
    print("====================================")
    print("Digite uma mensagem para enviar")
    print("Digite 'exit' para sair")
    print("====================================")


def build_zenoh_config(listen_endpoint, neighbor_endpoints, gossip_enabled):
    """
    Monta a configuração Zenoh em modo peer.

    A configuração mantém:
    - endpoint local de escuta;
    - endpoints configurados para conexão;
    - multicast desabilitado;
    - gossip configurável;
    - roteamento peer em modo linkstate.

    A aplicação não usa esses endpoints para escolher próximo salto.
    Eles são usados apenas pela infraestrutura Zenoh para formar a malha.
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


def load_outbox(outbox_file):
    """
    Carrega a outbox persistida em arquivo.

    Estrutura esperada:
    msg_id -> payload da mensagem pendente.
    """

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
    """
    Salva a outbox no arquivo local.
    """

    try:
        with open(outbox_file, "w", encoding="utf-8") as f:
            json.dump(outbox, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"{RED}[OUTBOX]{RESET} erro ao salvar arquivo: {e}")


def build_payload(node_id, short_id, target, user_message):
    """
    Monta a mensagem criada pelo operador.

    Payload gerado:
    - msg_id: identificador único da mensagem;
    - origin/origin_id: nó de origem;
    - target: destino lógico;
    - message: texto digitado;
    - created_at: momento de criação;
    - last_sent_at/attempts: controle local de retry.

    Não há path, hop, relay_by ou qualquer controle manual de rota.
    """

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
        "attempts": 0
    }


def enqueue_message(
    user_message,
    node_id,
    short_id,
    target,
    outbox_file,
    outbox_lock,
    runtime_flags
):
    """
    Adiciona uma mensagem digitada pelo usuário na outbox local.
    """

    payload = build_payload(node_id, short_id, target, user_message)

    with outbox_lock:
        outbox = load_outbox(outbox_file)
        outbox[payload["msg_id"]] = payload
        save_outbox(outbox_file, outbox)

    print(f"{BLUE}[QUEUE]{RESET} mensagem adicionada: {payload['msg_id']}")
    print(f"{BLUE}[QUEUE]{RESET} conteudo: {user_message}")

    runtime_flags["OUTBOX_CHECK"] = True


def remove_acked_message(msg_id, outbox_file, outbox_lock, runtime_flags):
    """
    Remove da outbox uma mensagem confirmada por ACK.
    """

    with outbox_lock:
        outbox = load_outbox(outbox_file)

        if msg_id in outbox:
            del outbox[msg_id]
            save_outbox(outbox_file, outbox)
            print(f"{GREEN}[ACK]{RESET} mensagem confirmada e removida da fila: {msg_id}")
            runtime_flags["OUTBOX_CHECK"] = True
        else:
            print(f"{RED}[ACK]{RESET} ACK recebido para msg_id desconhecido/removido: {msg_id}")


def mark_attempt(msg_id, outbox_file, outbox_lock):
    """
    Atualiza a tentativa de envio de uma mensagem pendente.
    """

    with outbox_lock:
        outbox = load_outbox(outbox_file)

        if msg_id not in outbox:
            return None

        outbox[msg_id]["attempts"] += 1
        outbox[msg_id]["last_sent_at"] = time.time()
        payload = dict(outbox[msg_id])

        save_outbox(outbox_file, outbox)
        return payload


def get_pending_messages(outbox_file, outbox_lock):
    """
    Retorna as mensagens ainda pendentes na outbox.
    """

    with outbox_lock:
        outbox = load_outbox(outbox_file)
        return list(outbox.values())


def is_own_heartbeat(data, node_id, short_id):
    """
    Verifica se o heartbeat recebido foi enviado pelo próprio operador.
    """

    sender_node = data.get("node")
    sender_short_id = data.get("short_id")

    return (
        sender_node in (node_id, short_id)
        or sender_short_id in (node_id, short_id)
    )


def update_peer_status(
    sender_node,
    sender_short_id,
    peers_status,
    peers_lock,
    runtime_flags
):
    """
    Atualiza o status de um peer quando um heartbeat válido é recebido.
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
        print(f"{GREEN}[LIVENESS]{RESET} Novo peer detectado: {sender_node} (short_id={sender_short_id})")
        runtime_flags["ONLINE_CHECK"] = True

    if was_offline:
        print(f"{GREEN}[LIVENESS]{RESET} {sender_node} voltou para ONLINE")
        runtime_flags["ONLINE_CHECK"] = True


def create_input_loop(
    stop_event,
    runtime_flags,
    node_id,
    short_id,
    target,
    outbox_file,
    outbox_lock
):
    """
    Cria a função responsável por ler mensagens digitadas pelo usuário.
    """

    def input_loop():
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

            if not raw_input_value:
                continue

            enqueue_message(
                raw_input_value,
                node_id,
                short_id,
                target,
                outbox_file,
                outbox_lock,
                runtime_flags
            )

    return input_loop


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
            sender_node,
            sender_short_id,
            peers_status,
            peers_lock,
            runtime_flags
        )

    return on_heartbeat


def create_ack_handler(outbox_file, outbox_lock, runtime_flags):
    """
    Cria o callback chamado quando um ACK é recebido.
    """

    def on_ack(sample):
        data = parse_payload(sample)

        msg_id = data.get("msg_id")
        ack = data.get("ack", False)

        if not msg_id or not ack:
            print(f"{RED}[ACK]{RESET} payload invalido: {data}")
            return

        remove_acked_message(
            msg_id,
            outbox_file,
            outbox_lock,
            runtime_flags
        )

    return on_ack


def build_send_payload(updated_payload):
    """
    Monta o payload efetivamente publicado no tópico de comandos.

    Este payload não contém informações de relay manual.
    A aplicação publica a mensagem no tópico configurado e o Zenoh fica
    responsável pela entrega conforme a malha/sessão configurada.
    """

    return {
        "msg_id": updated_payload["msg_id"],
        "origin": updated_payload["origin"],
        "origin_id": updated_payload["origin_id"],
        "target": updated_payload["target"],
        "message": updated_payload["message"],
        "created_at": updated_payload["created_at"],
        "sent_at": time.time(),
        "attempts": updated_payload["attempts"]
    }


def should_retry_message(message, now):
    """
    Verifica se uma mensagem pendente já pode ser reenviada.
    """

    last_sent_at = message.get("last_sent_at", 0)
    return now - last_sent_at >= RETRY_INTERVAL


def sender_loop(
    pub_command,
    stop_event,
    outbox_file,
    outbox_lock
):
    """
    Reenvia mensagens pendentes da outbox até que sejam confirmadas por ACK.

    O retry republica a mensagem no mesmo tópico de comando.
    Isso não é relay manual, pois a aplicação não encaminha mensagens recebidas
    nem escolhe próximo peer.
    """

    while not stop_event.is_set():
        pending_messages = get_pending_messages(outbox_file, outbox_lock)
        now = time.time()

        for msg in pending_messages:
            if not should_retry_message(msg, now):
                continue

            updated_payload = mark_attempt(
                msg["msg_id"],
                outbox_file,
                outbox_lock
            )

            if updated_payload is None:
                continue

            payload_to_send = build_send_payload(updated_payload)

            pub_command.put(json.dumps(payload_to_send))

        time.sleep(1)


def liveness_checker(
    stop_event,
    peers_status,
    peers_lock,
    runtime_flags
):
    """
    Marca peers como OFFLINE quando deixam de enviar heartbeat.
    """

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

                        runtime_flags["ONLINE_CHECK"] = True


def print_peers_status_if_needed(
    peers_status,
    peers_lock,
    runtime_flags
):
    """
    Imprime a tabela de status dos peers somente quando houve mudança.
    """

    with peers_lock:
        if runtime_flags["ONLINE_CHECK"]:
            runtime_flags["ONLINE_CHECK"] = False

            if peers_status:
                print("\n----------- STATUS DOS PEERS -----------")

                for peer, info in sorted(peers_status.items()):
                    if info["status"] == "ONLINE":
                        connected_status = "CONNECTED"
                    else:
                        connected_status = "NOT CONNECTED"

                    last_seen_ago = round(time.time() - info["last_seen"], 1)

                    print(
                        f"{peer:<10} | {connected_status} | "
                        f"ultimo heartbeat ha {last_seen_ago:>4}s"
                    )

                print("----------------------------------------")


def print_outbox_status(pending_messages):
    """
    Imprime o estado atual da outbox.
    """

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


def print_outbox_status_if_needed(
    pending_messages,
    runtime_flags
):
    """
    Imprime a outbox somente quando houve mudança na fila.
    """

    if runtime_flags["OUTBOX_CHECK"]:
        runtime_flags["OUTBOX_CHECK"] = False
        print_outbox_status(pending_messages)


def status_loop(
    stop_event,
    peers_status,
    peers_lock,
    outbox_file,
    outbox_lock,
    runtime_flags
):
    """
    Imprime status de peers e outbox quando há alterações relevantes.
    """

    while not stop_event.is_set():
        time.sleep(PRINT_STATUS_INTERVAL)

        print_peers_status_if_needed(
            peers_status,
            peers_lock,
            runtime_flags
        )

        pending_messages = get_pending_messages(outbox_file, outbox_lock)

        print_outbox_status_if_needed(
            pending_messages,
            runtime_flags
        )


def build_heartbeat_payload(node_id, short_id):
    """
    Monta o heartbeat publicado periodicamente pelo operador.
    """

    return {
        "node": node_id,
        "short_id": short_id,
        "timestamp": time.time()
    }


def publish_heartbeats(pub_heartbeat, stop_event, node_id, short_id):
    """
    Publica heartbeats enquanto o operador estiver ativo.
    """

    while not stop_event.is_set():
        heartbeat_payload = build_heartbeat_payload(node_id, short_id)
        pub_heartbeat.put(json.dumps(heartbeat_payload))
        time.sleep(HEARTBEAT_INTERVAL)


def start_background_threads(
    input_loop,
    pub_command,
    stop_event,
    outbox_file,
    outbox_lock,
    peers_status,
    peers_lock,
    runtime_flags
):
    """
    Inicia as threads auxiliares do operador.
    """

    threading.Thread(target=input_loop, daemon=True).start()

    threading.Thread(
        target=sender_loop,
        args=(
            pub_command,
            stop_event,
            outbox_file,
            outbox_lock
        ),
        daemon=True
    ).start()

    threading.Thread(
        target=liveness_checker,
        args=(
            stop_event,
            peers_status,
            peers_lock,
            runtime_flags
        ),
        daemon=True
    ).start()

    threading.Thread(
        target=status_loop,
        args=(
            stop_event,
            peers_status,
            peers_lock,
            outbox_file,
            outbox_lock,
            runtime_flags
        ),
        daemon=True
    ).start()


def run_node(cfg):
    """
    Prepara o operador, registra subscribers/publishers e inicia a execução.
    """

    node_id = cfg["node_id"]
    short_id = cfg["short_id"]
    listen_endpoint = cfg["listen_endpoint"]
    neighbor_endpoints = cfg["neighbor_endpoints"]
    gossip_enabled = cfg["gossip_enabled"]

    target = cfg.get("target", DEFAULT_TARGET)
    command_key = cfg.get("command_key", DEFAULT_COMMAND_KEY)
    ack_sub_key = cfg.get(
        "ack_sub_key",
        ACK_SUB_KEY_TEMPLATE.format(short_id=short_id)
    )
    outbox_file = cfg.get(
        "outbox_file",
        OUTBOX_FILE_TEMPLATE.format(short_id=short_id)
    )

    heartbeat_pub_key = HEARTBEAT_TOPIC_TEMPLATE.format(node_id=node_id)

    # Flags simples usadas pelas threads para decidir quando imprimir status.
    runtime_flags = {
        "ONLINE_CHECK": False,
        "OUTBOX_CHECK": False
    }

    # Guarda o último heartbeat recebido de cada peer conhecido.
    # Estrutura: peer_id -> short_id, last_seen e status ONLINE/OFFLINE.
    peers_status = {}

    # Protege o acesso ao peers_status, usado por callbacks e pela thread de liveness.
    peers_lock = threading.Lock()

    # Protege leitura e escrita da outbox persistida em arquivo.
    outbox_lock = threading.Lock()

    stop_event = threading.Event()

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
            create_heartbeat_handler(
                node_id,
                short_id,
                peers_status,
                peers_lock,
                runtime_flags
            )
        )

        zenoh_session.declare_subscriber(
            ack_sub_key,
            create_ack_handler(
                outbox_file,
                outbox_lock,
                runtime_flags
            )
        )

        pub_command = zenoh_session.declare_publisher(command_key)
        pub_heartbeat = zenoh_session.declare_publisher(heartbeat_pub_key)

        pending_messages = get_pending_messages(outbox_file, outbox_lock)
        print_outbox_status(pending_messages)

        input_loop = create_input_loop(
            stop_event,
            runtime_flags,
            node_id,
            short_id,
            target,
            outbox_file,
            outbox_lock
        )

        start_background_threads(
            input_loop,
            pub_command,
            stop_event,
            outbox_file,
            outbox_lock,
            peers_status,
            peers_lock,
            runtime_flags
        )

        try:
            publish_heartbeats(
                pub_heartbeat,
                stop_event,
                node_id,
                short_id
            )

        except KeyboardInterrupt:
            print(f"\nEncerrando {node_id}...")
            stop_event.set()


def main():
    if len(sys.argv) < 2:
        print("Uso: py peer_operador.py <config.json>")
        return

    config_path = sys.argv[1]
    cfg = load_config(config_path)

    run_node(cfg)


if __name__ == "__main__":
    main()