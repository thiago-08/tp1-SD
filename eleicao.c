#define THREADED
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zookeeper/zookeeper.h>
#include <sys/select.h> 

char meu_nome_global[512]; 
const char *caminho_mensagem = "/mensagem";
int sou_lider = 0; 

int comparar_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

void avaliar_lideranca(zhandle_t *zh);

void watcher_mensagem(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_CHANGED_EVENT) {
        char buffer_msg[1024];
        int tamanho_msg = sizeof(buffer_msg);
        
        int rc = zoo_wget(zh, caminho_mensagem, watcher_mensagem, NULL, buffer_msg, &tamanho_msg, NULL);
        if (rc == ZOK) {
            buffer_msg[tamanho_msg < sizeof(buffer_msg) ? tamanho_msg : sizeof(buffer_msg) - 1] = '\0';
            printf("\n[NOTIFICAÇÃO ZAB] -> Consenso atingido (Maioria OK). Valor sincronizado nos servidores!\n");
            printf("--------------------------------------------------\n");
            printf("VALOR ATUALIZADO: '%s'\n", buffer_msg);
            printf("--------------------------------------------------\n> ");
            fflush(stdout); 
        }
    }
}

void watcher_lideranca(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_DELETED_EVENT) {
        // CORREÇÃO 2: Só alerta ostensivamente se quem caiu foi o líder (candidato com menor número).
        // Extrai o número do nó que caiu para comparar se era o chefe
        char *nome_caiu = strrchr(path, '/');
        if (nome_caiu != NULL && strstr(nome_caiu, "0000000000") != NULL) {
            printf("\n[ALERTA DE FALHA] O LÍDER DO SISTEMA CAIU!\n");
        }
        
        // A reavaliação interna precisa ocorrer silenciosamente para manter a fila organizada
        avaliar_lideranca(zh);
    }
}

void watcher_global(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_SESSION_EVENT && state == ZOO_CONNECTED_STATE) {
        printf("Conectado ao cluster ZooKeeper!\n");
    }
}

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
        // Se eu assumi a liderança, mudo a flag e não declaro mais nada para não sujar a tela
        if (sou_lider == 0) {
            sou_lider = 1;
            printf("\n==================================================\n");
            printf("           SOU O NOVO LÍDER! (Nó: %s)\n", meu_nome_global);
            printf("==================================================\n> ");
        }
    } else if (meu_indice > 0) {
        sou_lider = 0;
        char caminho_anterior[512];
        snprintf(caminho_anterior, sizeof(caminho_anterior), "/eleicao/%s", filhos.data[meu_indice - 1]);
        
        // Se conectou pela primeira vez, imprime o status. Se for só reavaliação de fila, fica quieto.
        static int primeira_vez = 1;
        if (primeira_vez) {
            printf("\nSou Seguidor. Aguardando comandos...\n> ");
            primeira_vez = 0;
        }
        
        struct Stat stat;
        rc = zoo_wexists(zh, caminho_anterior, watcher_lideranca, NULL, &stat);
        if (rc == ZNONODE) avaliar_lideranca(zh); 
    }
    fflush(stdout);
}

int teclado_pressionado() {
    struct timeval tv;
    fd_set fds;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    FD_ZERO(&fds);
    FD_SET(STDIN_FILENO, &fds);
    select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv);
    return FD_ISSET(STDIN_FILENO, &fds);
}

int main() {
    zhandle_t *zh = zookeeper_init("localhost:2181,localhost:2182,localhost:2183", watcher_global, 10000, 0, 0, 0);
    if (!zh) return 1;
    sleep(1);

    zoo_create(zh, "/eleicao", "root", 4, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    zoo_create(zh, caminho_mensagem, "Vazio", 5, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);

    char buffer_msg[1024];
    int tamanho_msg = sizeof(buffer_msg);
    zoo_wget(zh, caminho_mensagem, watcher_mensagem, NULL, buffer_msg, &tamanho_msg, NULL);

    char meu_caminho[512];
    int rc = zoo_create(zh, "/eleicao/candidato-", "dados", 5, 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE, 
                        meu_caminho, sizeof(meu_caminho));
                        
    if (rc == ZOK) {
        char *nome_extraido = strrchr(meu_caminho, '/');
        if (nome_extraido != NULL) strcpy(meu_nome_global, nome_extraido + 1);
        avaliar_lideranca(zh);
    } else {
        return 1;
    }

    printf("\n--- Teste de Escrita Distribuída ---\n");
    printf("Digite qualquer palavra para testar o protocolo ZAB.\n> ");
    fflush(stdout);

    char input_teclado[256];
    while(1) {
        if (teclado_pressionado()) {
            if (fgets(input_teclado, sizeof(input_teclado), stdin) != NULL) {
                input_teclado[strcspn(input_teclado, "\n")] = 0; 
                
                if (strlen(input_teclado) > 0) {
                    char nova_msg_formatada[1024];
                    snprintf(nova_msg_formatada, sizeof(nova_msg_formatada), "[%s %s]: %s", 
                             sou_lider ? "Líder" : "Seguidor", meu_nome_global, input_teclado);
                    
                    // CORREÇÃO 1: Explicação visual do fluxo arquitetural
                    if (!sou_lider) {
                        printf("\n[LOG] Eu sou Seguidor. Encaminhando requisição de escrita ao Líder do Cluster...\n");
                    } else {
                        printf("\n[LOG] Eu sou o Líder. Coordenando votação da escrita com o cluster...\n");
                    }
                    printf("[LOG] Aguardando quórum...\n");
                    fflush(stdout);
                    
                    // Dispara a requisição para o Docker, que faz o consenso real.
                    zoo_set(zh, caminho_mensagem, nova_msg_formatada, strlen(nova_msg_formatada), -1);
                }
            }
        }
        usleep(100000); 
    }

    zookeeper_close(zh);
    return 0;
}