#define THREADED
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zookeeper/zookeeper.h>

char meu_nome_global[512]; //variável global que guarda o nome do nó (ex: candidato-0000000001)

//função de comparação para ordenar alfabeticamente
int comparar_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

void avaliar_lideranca(zhandle_t *zh);

//watcher para monitorar a queda do nó que está na frente
void watcher_lideranca(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_DELETED_EVENT) {
        printf("\n[ALERTA] O nó %s caiu!\nReavaliando a lista de candidatos...\n", path);
        avaliar_lideranca(zh);
    }
}

//watcher global 
void watcher_global(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            printf("Conectado ao cluster ZooKeeper!\n");
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            printf("Sessão expirada.\n");
        }
    }
}

//lógica principal da eleição de líder
void avaliar_lideranca(zhandle_t *zh) {
    struct String_vector filhos;
    int rc = zoo_get_children(zh, "/eleicao", 0, &filhos);
    if (rc != ZOK){
        fprintf(stderr, "Erro ao obter lista de candidatos. Código: %d\n", rc);
        return;
    }
    //ordena o vetor de strings 'filhos.data' para achar o menor número
    qsort(filhos.data, filhos.count, sizeof(char *), comparar_strings);
    //encontra qual é o índice do nó da lissssta ordenada
    int meu_indice = -1;
    for (int i = 0; i < filhos.count; i++) {
        if (strcmp(filhos.data[i], meu_nome_global) == 0) {
            meu_indice = i;
            break;
        }
    }
    if (meu_indice == -1) {
        fprintf(stderr, "Erro: Não encontrei o nó na lista do ZooKeeper!\n");
        return;
    }
    if (meu_indice == 0) {
        printf("\n==================================================\n");
        printf("           SOU O NOVO LÍDER! (Nó: %s)\n", meu_nome_global);
        printf("==================================================\n\n");
    } else {
        char caminho_anterior[512];
        snprintf(caminho_anterior, sizeof(caminho_anterior), "/eleicao/%s", filhos.data[meu_indice - 1]);
        printf("\nAinda não sou o líder. Observando a queda do nó: %s\n", caminho_anterior);
        //colocando um watch no nó anterior
        struct Stat stat;
        rc = zoo_wexists(zh, caminho_anterior, watcher_lideranca, NULL, &stat);
        //proteção contra race condition
        if (rc == ZNONODE) { //retorna ZNONODE se o nó cai ao mesmo tempo que pegamos a lista e tentamos colocar um watch
            printf("O nó anterior desapareceu antes de ser observado! Tentando novamente...\n");
            avaliar_lideranca(zh); 
        }
    }
}

int main() {
    zhandle_t *zh = zookeeper_init("localhost:2181,localhost:2182,localhost:2183", watcher_global, 10000, 0, 0, 0);
    if (!zh) {
        fprintf(stderr, "Erro ao conectar no ZooKeeper\n");
        return 1;
    }
    sleep(1);
    //garante que o diretório pai "/eleicao" exista 
    zoo_create(zh, "/eleicao", "root", 4, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    char meu_caminho[512];
    // Cria o znode efêmero e sequencial
    int rc = zoo_create(zh, "/eleicao/candidato-", "dados", 5, 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE, 
                        meu_caminho, sizeof(meu_caminho));
                        
    if (rc == ZOK) {
        printf("Entrei na eleição. Meu caminho completo: %s\n", meu_caminho);
        char *nome_extraido = strrchr(meu_caminho, '/');
        if (nome_extraido != NULL) {
            strcpy(meu_nome_global, nome_extraido + 1);
        }
        //analisa qual deve ser o líder
        avaliar_lideranca(zh);
    } else {
        fprintf(stderr, "Falha ao criar o nó. Código de erro: %d\n", rc);
        zookeeper_close(zh);
        return 1;
    }
    // Mantém o programa rodando para não perder a conexão
    while(1) {
        sleep(1);
    }
    zookeeper_close(zh);
    return 0;
}