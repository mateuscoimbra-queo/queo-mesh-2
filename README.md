# DSOT - Mesh Zenoh

Projeto experimental de conectividade mesh usando Zenoh para validar comunicação distribuída entre nós, monitoramento de liveness, retransmissão de comandos, ACKs e roteamento em malha.

## Visão geral

Este repositório implementa várias variantes de nós Zenoh em modo `peer`, com foco em:

- Heartbeat e detecção de liveness entre peers
- Configuração de nós via JSON
- Rede mesh com roteamento `linkstate`
- Agenda de retransmissão de comandos e buffer de relay
- Recebimento de mensagens com ACKs e deduplicação
- Controle de execução via script Windows `dsot.bat`

## Estrutura do repositório

- `peer_no_relay.py` - nó simples que publica heartbeats, escuta outros peers e marca peers OFFLINE sem relay de mensagens.
- `peer_generic.py` - nó genérico com heartbeat, relay de comandos, ACKs, deduplicação e reenvio.
- `peer_operador.py` - nó operador que aceita entrada de usuário, coloca mensagens em outbox e retransmite a rede até o alvo.
- `peer_terminal.py` - nó terminal que recebe comandos, detecta duplicatas e responde com ACKs.
- `dsot.bat` - script Windows para abrir e fechar peers em janelas separadas.
- `configs/` - configurações JSON de cada peer.
- `peer0_outbox.json` - arquivo de armazenamento local para mensagens pendentes do operador.
- `main.py` - exemplo mínimo, não é usado pelo fluxo principal.

## Requisitos

- Python 3.x
- Biblioteca `zenoh` instalada no ambiente Python
- Windows PowerShell / CMD para executar `dsot.bat` (o script foi criado para Windows)

## Como executar

### Via `dsot.bat`

1. Abra uma janela de terminal em `c:\Users\COLABORADOR QUEO.COLABORADOR.000\zenoh\dsot`.
2. Execute `dsot.bat`.
3. Use os comandos interativos:
   - `open p0`, `close p0`
   - `open p1`, `close p1`
   - ...
   - `open pT`, `close pT`
   - `exit`

O script abre cada peer em uma nova janela CMD e fecha pelo título da janela.

### Executando um peer individualmente

Use o comando `py` seguido do arquivo do peer e do caminho do arquivo de configuração:

- `py peer_no_relay.py configs\peerX.json`
- `py peer_generic.py configs\peerX.json`
- `py peer_operador.py configs\peerX.json`
- `py peer_terminal.py configs\peerT.json`

Substitua `peerX.json` pelo arquivo desejado.

## Configuração de nó

Cada arquivo JSON em `configs/` define o comportamento de um nó.
Campos principais:

- `node_id` - identificador de nó completo usado no heartbeat.
- `short_id` - identificador curto usado em relatórios e caminhos de relay.
- `listen_endpoint` - endpoint local Zenoh para escuta.
- `neighbor_endpoints` - lista de endpoints dos peers vizinhos.
- `neighbor_nodes` - lista opcional de short IDs de peers vizinhos para alertas de offline.
- `gossip_enabled` - habilita/disabilita gossip no scouting.
- `target` - usado pelo operador para definir destino de comandos.
- `command_key` - tópico usado para publicar comandos.
- `ack_sub_key` - tópico usado pelo operador para receber ACKs.
- `outbox_file` - arquivo local para armazenar mensagens pendentes.

### Exemplo de configuração

```json
{
  "node_id": "peer_operador",
  "short_id": "peer0",
  "listen_endpoint": "tcp/0.0.0.0:7440",
  "neighbor_endpoints": [
    "tcp/127.0.0.1:7441",
    "tcp/127.0.0.1:7444",
    "tcp/127.0.0.1:7447"
  ],
  "neighbor_nodes": [
    "p1",
    "p4",
    "p7"
  ],
  "gossip_enabled": false,
  "target": "peer-T",
  "command_key": "dsot/peer-T/commands",
  "ack_sub_key": "dsot/acks/peer0",
  "outbox_file": "peer0_outbox.json"
}
```

## Descrição dos módulos

### `peer_no_relay.py`

- Abre sessão Zenoh em `peer` mode.
- Publica heartbeat em `dsot/{node_id}/heartbeat`.
- Inscreve-se em `dsot/*/heartbeat`.
- Ignora heartbeats do próprio nó.
- Marca peers como `ONLINE` quando recebe heartbeat.
- Marca peers como `OFFLINE` quando não recebe heartbeat em `PEER_TIMEOUT_SECONDS`.

### `peer_generic.py`

- Suporta heartbeat e liveness como `peer_no_relay.py`.
- Escuta comandos em `dsot/peer-T/commands`.
- Mantém um buffer de relay com deduplicação (`seen_messages`).
- Envia comandos para a rede e aguarda ACKs.
- Remove mensagens do buffer quando recebe ACK correspondente.
- Limpa caches de mensagens e ACKs antigos automaticamente.
- Exibe tabelas periódicas de status de peers e buffer.

### `peer_operador.py`

- Interface de operador para digitar mensagens.
- Armazena mensagens no `outbox_file` local.
- Reenvia mensagens periodicamente até receber ACK.
- Detecta peers OFFLINE e imprime estados de conectividade.
- Usa um fluxo de publicação de comandos e subscrição de ACKs.

### `peer_terminal.py`

- Nó terminal que recebe comandos destinados a `peer-T`.
- Armazena IDs de mensagens processadas para evitar duplicatas.
- Envia ACKs de volta ao remetente usando `dsot/acks/{origin_id}`.
- Mostra status de peers e informações da última mensagem recebida.

## Execução típica

1. Abra `peer_operador.py` ou `peer_generic.py` para participar da malha.
2. Abra `peer_terminal.py` para receber comandos destinados a `peer-T`.
3. Verifique que cada nó publica heartbeats e detecta peers ONLINE/OFFLINE.
4. Envie mensagens pelo operador e observe retransmissões e ACKs.

## Observações importantes

- A rede é construída com Zenoh em modo `peer`, então cada nó se conecta apenas aos vizinhos configurados.
- `gossip_enabled` pode ser ativado para testar descoberta e propagação alternativas.
- `dsot.bat` foi criado para facilitar testes no Windows com múltiplas janelas.
- `main.py` não faz parte do fluxo principal de malha; é um exemplo de boilerplate.

## Possíveis melhorias

- Adicionar documentação de topologia de rede e diagramas de peers.
- Incluir testes automatizados de mensagens e liveness.
- Implementar mecanismo de retransmissão com contagem de hops ou TTL.
- Adicionar suporte a configuração dinâmica de peers.
- Criar interface Web / visualização de estado em tempo real.

## Licença

Sem licença explícita definida no repositório.
