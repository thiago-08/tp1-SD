#define THREADED
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zookeeper/zookeeper.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/*
 * IMPORTANTE: existem DUAS eleições diferentes acontecendo aqui.
 *
 * 1) Eleição dos SERVIDORES ZooKeeper (zoo1, zoo2, zoo3), feita internamente
 *    pelo próprio ZooKeeper (Fast Leader Election + ZAB). É esse líder que
 *    coordena as escritas. O comando "status" mostra quem é o líder/seguidor.
 *
 * 2) Eleição entre os PROCESSOS CLIENTES (este programa), implementada com a
 *    receita clássica de znodes efêmeros sequenciais em /eleicao. Ser "líder"
 *    aqui NÃO muda nada no caminho das escritas.
 */

static const char *HOSTS = "localhost:2181,localhost:2182,localhost:2183";
static const int PORTAS[] = {2181, 2182, 2183};

char meu_nome_global[512];
const char *caminho_mensagem = "/mensagem";
volatile int sou_lider = 0;          // lido na thread principal e escrito na thread de callbacks
volatile int conectado = 0;

int comparar_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

void avaliar_lideranca(zhandle_t *zh);

/* ---------- Watchers ---------- */

void watcher_mensagem(zhandle_t *zh, int type, int state, const char *path, void *ctx) {
    (void)state; (void)path; (void)ctx;
    if (type != ZOO_CHANGED_EVENT) return;

    char buffer_msg[1024];
    int tamanho_msg = sizeof(buffer_msg) - 1;
    // Watch é de disparo único: relê o valor e registra de novo
    int rc = zoo_wget(zh, caminho_mensagem, watcher_mensagem, NULL, buffer_msg, &tamanho_msg, NULL);
    if (rc == ZOK) {
        if (tamanho_msg < 0) tamanho_msg = 0;          // nó sem dados
        buffer_msg[tamanho_msg] = '\0';
        printf("\n[NOTIFICAÇÃO] O valor de %s mudou (escrita já confirmada pelo quórum):\n", caminho_mensagem);
        printf("--------------------------------------------------\n");
        printf("VALOR ATUALIZADO: '%s'\n", buffer_msg);
        printf("--------------------------------------------------\n> ");
        fflush(stdout);
    }
}

void watcher_lideranca(zhandle_t *zh, int type, int state, const char *path, void *ctx) {
    (void)state; (void)ctx;
    if (type != ZOO_DELETED_EVENT) return;

    // Só recebo este evento do nó imediatamente à minha frente. Se depois de
    // reavaliar a fila eu virei líder, então quem caiu era o líder
    // (funciona para qualquer número de sequência, não só o 0000000000).
    int era_lider = sou_lider;
    avaliar_lideranca(zh);
    if (!era_lider && sou_lider) {
        printf("[ALERTA DE FALHA] O líder anterior dos clientes (%s) caiu e eu assumi.\n> ", path);
        fflush(stdout);
    }
}

void watcher_global(zhandle_t *zh, int type, int state, const char *path, void *ctx) {
    (void)path; (void)ctx;
    if (type != ZOO_SESSION_EVENT) return;

    if (state == ZOO_CONNECTED_STATE) {
        conectado = 1;
        const char *srv = zoo_get_current_server(zh);
        printf("\nConectado ao cluster ZooKeeper (servidor %s)\n", srv ? srv : "?");
    } else if (state == ZOO_CONNECTING_STATE) {
        printf("\n[AVISO] Conexão com o servidor perdida. Reconectando em outro servidor...\n");
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
        printf("\n[ERRO] Sessão expirou: meu nó efêmero foi apagado. Reinicie o programa.\n");
        exit(1);
    }
    fflush(stdout);
}

/* ---------- Eleição entre clientes ---------- */

void avaliar_lideranca(zhandle_t *zh) {
    struct String_vector filhos;
    int rc = zoo_get_children(zh, "/eleicao", 0, &filhos);
    if (rc != ZOK) return;

    qsort(filhos.data, filhos.count, sizeof(char *), comparar_strings);

    int meu_indice = -1;
    for (int i = 0; i < filhos.count; i++) {
        if (strcmp(filhos.data[i], meu_nome_global) == 0) {
            meu_indice = i;
            break;
        }
    }

    if (meu_indice == 0) {
        if (!sou_lider) {
            sou_lider = 1;
            printf("\n==================================================\n");
            printf("   SOU O LÍDER DOS CLIENTES! (Nó: %s)\n", meu_nome_global);
            printf("==================================================\n> ");
        }
    } else if (meu_indice > 0) {
        sou_lider = 0;

        char caminho_anterior[512];
        snprintf(caminho_anterior, sizeof(caminho_anterior), "/eleicao/%s", filhos.data[meu_indice - 1]);

        static int primeira_vez = 1;
        if (primeira_vez) {
            printf("\nSou Seguidor (líder atual: %s). Vigiando %s\n> ", filhos.data[0], caminho_anterior);
            primeira_vez = 0;
        }

        struct Stat stat;
        rc = zoo_wexists(zh, caminho_anterior, watcher_lideranca, NULL, &stat);
        if (rc == ZNONODE) {                       // antecessor sumiu antes do watch
            deallocate_String_vector(&filhos);
            avaliar_lideranca(zh);
            return;
        }
    } else {
        printf("\n[ERRO] Meu nó %s não existe mais em /eleicao.\n> ", meu_nome_global);
    }
    fflush(stdout);
    deallocate_String_vector(&filhos);
}

/* ---------- Status dos servidores (eleição real do ZooKeeper) ---------- */

// Envia o comando de 4 letras "srvr" e extrai a linha "Mode: leader/follower"
void modo_servidor(int porta, char *saida, size_t tam) {
    snprintf(saida, tam, "FORA DO AR");
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return;
    struct timeval tv = {1, 0};
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(porta);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(s, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
        char buf[2048] = {0};
        int total = 0, n;
        write(s, "srvr", 4);
        while (total < (int)sizeof(buf) - 1 && (n = read(s, buf + total, sizeof(buf) - 1 - total)) > 0)
            total += n;
        char *m = strstr(buf, "Mode: ");
        if (m) {
            m += 6;
            size_t len = strcspn(m, "\r\n");
            snprintf(saida, tam, "%.*s", (int)len, m);
        } else if (total > 0) {
            snprintf(saida, tam, "sem quórum / 4lw não liberado");
        }
    }
    close(s);
}

void mostrar_status(zhandle_t *zh) {
    char modo[64];
    const char *atual = zoo_get_current_server(zh);
    printf("\n=========== STATUS DO ENSEMBLE ZOOKEEPER ===========\n");
    for (int i = 0; i < 3; i++) {
        modo_servidor(PORTAS[i], modo, sizeof(modo));
        printf("  servidor %d (porta %d): %s\n", i + 1, PORTAS[i], modo);
    }
    printf("  Este cliente está conectado em: %s\n", atual ? atual : "nenhum");
    printf("  Papel deste cliente na eleição de clientes: %s (%s)\n",
           sou_lider ? "LÍDER" : "Seguidor", meu_nome_global);
    printf("====================================================\n> ");
    fflush(stdout);
}

/* ---------- Main ---------- */

int teclado_pressionado(void) {
    struct timeval tv = {0, 0};
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(STDIN_FILENO, &fds);
    select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv);
    return FD_ISSET(STDIN_FILENO, &fds);
}

int main(void) {
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    zhandle_t *zh = zookeeper_init(HOSTS, watcher_global, 10000, 0, 0, 0);
    if (!zh) { perror("zookeeper_init"); return 1; }

    // Espera a conexão de verdade em vez de um sleep fixo
    for (int i = 0; i < 100 && !conectado; i++) usleep(100000);
    if (!conectado) {
        fprintf(stderr, "Não consegui conectar em %s (o cluster está no ar?)\n", HOSTS);
        return 1;
    }

    int rc = zoo_create(zh, "/eleicao", "root", 4, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    if (rc != ZOK && rc != ZNODEEXISTS) { fprintf(stderr, "Erro criando /eleicao: %s\n", zerror(rc)); return 1; }
    rc = zoo_create(zh, caminho_mensagem, "Vazio", 5, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    if (rc != ZOK && rc != ZNODEEXISTS) { fprintf(stderr, "Erro criando %s: %s\n", caminho_mensagem, zerror(rc)); return 1; }

    char buffer_msg[1024];
    int tamanho_msg = sizeof(buffer_msg) - 1;
    if (zoo_wget(zh, caminho_mensagem, watcher_mensagem, NULL, buffer_msg, &tamanho_msg, NULL) == ZOK) {
        if (tamanho_msg < 0) tamanho_msg = 0;
        buffer_msg[tamanho_msg] = '\0';
        printf("Valor atual de %s: '%s'\n", caminho_mensagem, buffer_msg);
    }

    char meu_caminho[512];
    rc = zoo_create(zh, "/eleicao/candidato-", "dados", 5,
                    &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE,
                    meu_caminho, sizeof(meu_caminho));
    if (rc != ZOK) { fprintf(stderr, "Erro criando candidato: %s\n", zerror(rc)); return 1; }

    char *nome_extraido = strrchr(meu_caminho, '/');
    snprintf(meu_nome_global, sizeof(meu_nome_global), "%s", nome_extraido ? nome_extraido + 1 : meu_caminho);
    avaliar_lideranca(zh);

    printf("\n--- Teste de Escrita Distribuída ---\n");
    printf("Digite um texto para gravar em %s, 'status' para ver os servidores ou 'sair'.\n> ", caminho_mensagem);
    fflush(stdout);

    char input_teclado[256];
    while (1) {
        if (teclado_pressionado()) {
            if (fgets(input_teclado, sizeof(input_teclado), stdin) == NULL) break;   // EOF (Ctrl+D)
            input_teclado[strcspn(input_teclado, "\n")] = 0;
            if (strlen(input_teclado) == 0) continue;
            if (strcmp(input_teclado, "sair") == 0) break;
            if (strcmp(input_teclado, "status") == 0) { mostrar_status(zh); continue; }

            char nova_msg[1024];
            snprintf(nova_msg, sizeof(nova_msg), "[%s %s]: %s",
                     sou_lider ? "Líder" : "Seguidor", meu_nome_global, input_teclado);

            const char *srv = zoo_get_current_server(zh);
            printf("\n[LOG] Enviando escrita ao servidor %s.\n", srv ? srv : "?");
            printf("[LOG] Se esse servidor for seguidor, ele repassa ao servidor líder, que\n"
                   "      propõe a escrita (ZAB) e só confirma quando a maioria (2 de 3) aceitar.\n");
            fflush(stdout);

            rc = zoo_set(zh, caminho_mensagem, nova_msg, strlen(nova_msg), -1);
            if (rc == ZOK) {
                printf("[LOG] Escrita CONFIRMADA pelo quórum.\n> ");
            } else {
                printf("[ERRO] Escrita NÃO confirmada: %s (o cluster tem quórum?)\n> ", zerror(rc));
            }
            fflush(stdout);
        }
        usleep(100000);
    }

    zookeeper_close(zh);   // fecha a sessão: o nó efêmero some na hora e o próximo assume
    return 0;
}