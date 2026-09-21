# TP1 – Sistemas Distribuídos: Eleição de Líder com ZooKeeper

Um cluster de **3 servidores ZooKeeper** (zoo1, zoo2, zoo3) roda em Docker. Um programa em C (`eleicao.c`) se conecta a esse cluster e mostra:

- qual servidor é o **líder** e quais são **seguidores**;
- a **eleição de líder entre os clientes**, feita com nós efêmeros sequenciais em `/eleicao`;
- a **alteração de um dado compartilhado** (a string em `/mensagem`), confirmada pelo quórum dos servidores com o protocolo ZAB.

---

## Pré-requisitos

- Docker com o Docker Compose
- GCC
- Biblioteca C do ZooKeeper (versão multithread)

No Ubuntu/Debian:

```bash
sudo apt install gcc libzookeeper-mt-dev
```

---

## Como executar

### 1. Subir o cluster de servidores

```bash
docker compose up -d
```

Espere alguns segundos para os 3 servidores fazerem a eleição entre eles.

### 2. Compilar o programa

```bash
gcc eleicao.c -o eleicao -lzookeeper_mt
```

### 3. Rodar o programa

```bash
./eleicao
```

Para ver a eleição entre clientes, abra **vários terminais** e rode `./eleicao` em cada um. O primeiro a conectar vira líder dos clientes, e os outros ficam como seguidores.

---

## Comandos dentro do programa

| Comando         | O que faz                                                                  |
|-----------------|----------------------------------------------------------------------------|
| *qualquer texto* | Grava o texto em `/mensagem`. Todos os clientes recebem o novo valor.      |
| `status`        | Mostra qual servidor é líder, quais são seguidores e quais estão fora do ar. |
| `sair`          | Encerra o programa.                                                         |

---

## Simulando falhas

### Derrubar um servidor

```bash
docker compose stop zoo3
```

> Troque o número depois de `zoo` pelo servidor que quer derrubar: `zoo1`, `zoo2` ou `zoo3`.

### Religar o servidor

```bash
docker compose start zoo3
```

---

## Estrutura

```
.
├── docker-compose.yml   # cluster com 3 servidores ZooKeeper (portas 2181, 2182, 2183)
├── eleicao.c            # cliente: eleição, escrita em /mensagem e comando status
└── README.md
```