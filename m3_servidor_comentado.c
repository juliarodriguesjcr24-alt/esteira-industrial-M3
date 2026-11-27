#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sched.h>
#include <stdatomic.h>

// ============================================================
// M3 - SERVIDOR PERIODICO PARA A TAREFA APERIODICA SORT_ACT
// Sistemas em Tempo Real
//
// Evolucao do trabalho M2 da esteira industrial.
//
// Ideia principal:
// - ENC_SENSE e SPD_CTRL representam tarefas periodicas;
// - SORT_ACT representa uma tarefa aperiódica;
// - as requisicoes SORT nao executam diretamente quando chegam;
// - elas entram em uma fila;
// - um servidor periodico acorda a cada Ts e atende a fila
//   enquanto ainda possuir orcamento Cs.
//
// Assim, o consumo de CPU das requisicoes aperiódicas fica
// limitado aproximadamente pela razao Cs/Ts.
// ============================================================

// ============================================================
// CONSTANTES DE TEMPO E PARAMETROS DAS TAREFAS
// ============================================================

#define NS_PER_MS 1000000L

// ENC_SENSE: periodo = deadline = 5 ms
#define ENC_T_NS      (5L  * NS_PER_MS) //proposta da m2
#define ENC_D_NS      (5L  * NS_PER_MS)

// SPD_CTRL: periodo = deadline = 10 ms
#define CTRL_T_NS     (10L * NS_PER_MS) //era acionada pela enc sense
#define CTRL_D_NS     (10L * NS_PER_MS)

// SORT_ACT: deadline = 10 ms
#define SORT_D_NS     (10L * NS_PER_MS)

// Custos computacionais simulados, reaproveitando a ideia da M2 - código de referência da m2
#define ENC_C_US      700
#define CTRL_C_US     1200
#define SORT_C_US     700

// Limite de ativacoes das tarefas periodicas durante o experimento.
#define TOTAL_PERIODIC_JOBS 400

// ============================================================
// ESTRUTURAS DE DADOS
// ============================================================

// Cada requisicao aperiódica SORT guarda:
// - identificador;
// - instante em que o evento foi gerado;
// - ponteiro para a proxima requisicao da fila.
typedef struct sort_request {
    int id;
    struct timespec t_event;
    struct sort_request *next;
} sort_request_t;

// Parametros do servidor periodico:
// Ts = periodo do servidor;
// Cs = orcamento maximo de CPU por ativacao.
typedef struct {
    long period_ns;
    long budget_ns;
} server_params_t;

// Metricas usadas pelas tarefas periodicas.
typedef struct {
    uint64_t jobs;
    uint64_t misses;
    int64_t max_response_us;
} periodic_metrics_t;

// Metricas da tarefa aperiódica e do servidor.
typedef struct {
    uint64_t generated;
    uint64_t served;
    uint64_t misses;
    int64_t max_wait_us;
    int64_t max_response_us;
    uint64_t budget_exhaustions;
    uint64_t max_queue_len;
} sort_metrics_t;

// ============================================================
// FILA COMPARTILHADA DE REQUISICOES APERIODICAS
// ============================================================

static sort_request_t *queue_head = NULL;
static sort_request_t *queue_tail = NULL;
static uint64_t queue_len = 0;

// O mutex protege a fila contra acessos concorrentes entre
// o gerador de eventos e o servidor periodico.
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

// Flags de encerramento do experimento.
static atomic_bool generator_finished = false;
static atomic_bool stop_periodic = false;

// Estruturas globais de metricas.
static periodic_metrics_t enc_metrics = {0};
static periodic_metrics_t ctrl_metrics = {0};
static sort_metrics_t sort_metrics = {0};

// ============================================================
// FUNCOES AUXILIARES DE TEMPO
// ============================================================

// Calcula a diferenca entre dois instantes em nanossegundos.
static int64_t diff_ns(const struct timespec *end,
                       const struct timespec *start)
{
    return ((int64_t)(end->tv_sec - start->tv_sec) * 1000000000LL) +
           (int64_t)(end->tv_nsec - start->tv_nsec);
}

// Soma um intervalo em nanossegundos a um timespec.
// Esta funcao e usada para calcular as proximas liberacoes
// absolutas das threads periodicas.
static void timespec_add_ns(struct timespec *t, long ns)
{
    t->tv_nsec += ns;

    while (t->tv_nsec >= 1000000000L) {
        t->tv_nsec -= 1000000000L;
        t->tv_sec += 1;
    }
}

// Busy wait usado apenas para simular custo computacional.
// Em uma aplicacao real, este trecho seria substituido pelo
// processamento efetivamente realizado por cada tarefa.
static void busy_wait_us(long us)
{
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);

    const int64_t target_ns = us * 1000LL;

    do {
        clock_gettime(CLOCK_MONOTONIC, &now);
    } while (diff_ns(&now, &start) < target_ns);
}

// ============================================================
// OPERACOES DA FILA APERIODICA
// ============================================================

// Insere uma nova requisicao SORT na fila.
static void enqueue_sort(int id)
{
    sort_request_t *req = malloc(sizeof(sort_request_t));

    if (!req) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }

    req->id = id;
    clock_gettime(CLOCK_MONOTONIC, &req->t_event);
    req->next = NULL;

    pthread_mutex_lock(&queue_mutex);

    if (queue_tail) {
        queue_tail->next = req;
    } else {
        queue_head = req;
    }

    queue_tail = req;
    queue_len++;

    // Guarda o maior tamanho de fila observado.
    if (queue_len > sort_metrics.max_queue_len) {
        sort_metrics.max_queue_len = queue_len;
    }

    sort_metrics.generated++;

    pthread_mutex_unlock(&queue_mutex);
}

// Retira a requisicao mais antiga da fila (FIFO).
static sort_request_t *dequeue_sort(void)
{
    pthread_mutex_lock(&queue_mutex);

    sort_request_t *req = queue_head;

    if (req) {
        queue_head = req->next;

        if (!queue_head) {
            queue_tail = NULL;
        }

        queue_len--;
    }

    pthread_mutex_unlock(&queue_mutex);

    return req;
}

// Consulta o tamanho atual da fila de forma protegida.
static uint64_t get_queue_len(void)
{
    pthread_mutex_lock(&queue_mutex);
    uint64_t len = queue_len;
    pthread_mutex_unlock(&queue_mutex);

    return len;
}

// ============================================================
// SORT_ACT - TAREFA APERIODICA DA ESTEIRA
// ============================================================

// Esta funcao representa a mesma ideia da SORT_ACT da M2.
// A diferenca e que agora ela nao possui uma thread propria:
// ela e executada pelo servidor periodico quando sua requisicao
// e retirada da fila.
static void execute_sort(sort_request_t *req)
{
    struct timespec t_start, t_end;

    // Momento em que o servidor realmente inicia o atendimento.
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    // Tempo de espera = inicio do atendimento - chegada do evento.
    int64_t wait_us =
        diff_ns(&t_start, &req->t_event) / 1000LL;

    // Simula o custo de aproximadamente 0,7 ms da SORT_ACT da M2.
    busy_wait_us(SORT_C_US);

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    // Tempo de resposta completo = fim do atendimento - chegada.
    int64_t response_us =
        diff_ns(&t_end, &req->t_event) / 1000LL;

    sort_metrics.served++;

    // Miss se a resposta ultrapassar o deadline de 10 ms.
    if (response_us > (SORT_D_NS / 1000LL)) {
        sort_metrics.misses++;
    }

    if (wait_us > sort_metrics.max_wait_us) {
        sort_metrics.max_wait_us = wait_us;
    }

    if (response_us > sort_metrics.max_response_us) {
        sort_metrics.max_response_us = response_us;
    }

    // Mostra apenas algumas requisicoes para manter o terminal legivel.
    if (req->id <= 5 || req->id % 20 == 0) {
        printf("[SORT] id=%d | espera=%ld us | resposta=%ld us | %s\n",
               req->id,
               (long)wait_us,
               (long)response_us,
               response_us > (SORT_D_NS / 1000LL) ? "MISS" : "OK");
    }
}

// ============================================================
// THREAD DO SERVIDOR PERIODICO
// ============================================================

static void *server_thread(void *arg)
{
    server_params_t *params = (server_params_t *)arg;

    const long Ts = params->period_ns;
    const long Cs = params->budget_ns;

    struct timespec next_release;
    clock_gettime(CLOCK_MONOTONIC, &next_release);

    printf("[SERVER] Ts=%.2f ms | Cs=%.2f ms | Us=%.1f%%\n",
           Ts / 1e6,
           Cs / 1e6,
           100.0 * ((double)Cs / (double)Ts));

    for (;;) {
        // A cada ativacao, o orcamento consumido volta para zero.
        int64_t consumed_ns = 0;
        bool served_any = false;

        // O servidor continua atendendo enquanto existir orcamento.
        while (consumed_ns < Cs) {
            sort_request_t *req = dequeue_sort();

            // Se nao ha requisicoes, encerra o atendimento deste periodo.
            if (!req) {
                break;
            }

            served_any = true;

            // Mede o tempo efetivamente gasto pelo servidor no job.
            struct timespec before, after;
            clock_gettime(CLOCK_MONOTONIC, &before);

            execute_sort(req);

            clock_gettime(CLOCK_MONOTONIC, &after);
            consumed_ns += diff_ns(&after, &before);

            free(req);

            // Quando o orcamento acaba, o servidor para de atender
            // e so volta a executar na proxima liberacao periodica.
            if (consumed_ns >= Cs) {
                sort_metrics.budget_exhaustions++;
                break;
            }
        }

        // Se ainda restaram jobs na fila, eles aguardarao a proxima ativacao.
        if (served_any && get_queue_len() > 0) {
            printf("[SERVER] orcamento usado | fila restante=%lu\n",
                   (unsigned long)get_queue_len());
        }

        // Quando nao ha mais eventos a gerar e a fila ficou vazia,
        // o servidor conclui sua execucao experimental.
        if (generator_finished && get_queue_len() == 0) {
            break;
        }

        // Proxima liberacao absoluta do servidor.
        // TIMER_ABSTIME evita acumulo de drift entre ativacoes.
        timespec_add_ns(&next_release, Ts);

        clock_nanosleep(CLOCK_MONOTONIC,
                        TIMER_ABSTIME,
                        &next_release,
                        NULL);
    }

    return NULL;
}

// ============================================================
// TAREFAS PERIODICAS REPRESENTATIVAS DA ESTEIRA
// ============================================================

typedef struct {
    const char *name;
    long period_ns;
    long deadline_ns;
    long computation_us;
    periodic_metrics_t *metrics;
} periodic_params_t;

// Uma mesma funcao e usada para ENC_SENSE e SPD_CTRL.
// Os parametros recebidos definem periodo, deadline e custo.
static void *periodic_thread(void *arg)
{
    periodic_params_t *p = (periodic_params_t *)arg;

    struct timespec next_release;
    clock_gettime(CLOCK_MONOTONIC, &next_release);

    for (int i = 0;
         i < TOTAL_PERIODIC_JOBS && !stop_periodic;
         i++) {

        // O instante esperado da ativacao serve como referencia
        // para medir o tempo de resposta observado.
        struct timespec expected_release = next_release;

        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        // Simulacao do processamento da tarefa periodica.
        busy_wait_us(p->computation_us);

        clock_gettime(CLOCK_MONOTONIC, &end);

        int64_t response_ns =
            diff_ns(&end, &expected_release);

        int64_t response_us =
            response_ns / 1000LL;

        p->metrics->jobs++;

        if (response_ns > p->deadline_ns) {
            p->metrics->misses++;
        }

        if (response_us > p->metrics->max_response_us) {
            p->metrics->max_response_us = response_us;
        }

        // Mantem a periodicidade usando liberacoes absolutas.
        timespec_add_ns(&next_release, p->period_ns);

        clock_nanosleep(CLOCK_MONOTONIC,
                        TIMER_ABSTIME,
                        &next_release,
                        NULL);
    }

    return NULL;
}

// ============================================================
// GERADOR DE REQUISICOES APERIODICAS
// ============================================================

// Cenarios disponiveis:
// 1 -> carga leve: 20 eventos, 30 ms entre chegadas;
// 2 -> rajada intensa: 60 eventos, 1 ms entre chegadas;
// 3 -> carga mista: pequenas rajadas separadas por pausas.
typedef struct {
    int scenario;
} generator_params_t;

static void *generator_thread(void *arg)
{
    generator_params_t *p = (generator_params_t *)arg;
    int id = 1;

    // Pequena espera para permitir que as demais threads iniciem.
    usleep(55000);

    if (p->scenario == 1) {
        printf("[GERADOR] Cenario 1 - carga leve\n");

        for (int i = 0; i < 20; i++) {
            enqueue_sort(id++);
            usleep(30000);
        }
    }
    else if (p->scenario == 2) {
        printf("[GERADOR] Cenario 2 - rajada intensa\n");

        for (int i = 0; i < 60; i++) {
            enqueue_sort(id++);
            usleep(1000);
        }
    }
    else {
        printf("[GERADOR] Cenario 3 - carga mista\n");

        for (int burst = 0; burst < 6; burst++) {
            for (int i = 0; i < 8; i++) {
                enqueue_sort(id++);
                usleep(1000);
            }

            usleep(25000);
        }
    }

    generator_finished = true;
    return NULL;
}

// ============================================================
// CRIACAO DAS THREADS COM PRIORIDADE DE TEMPO REAL
// ============================================================

// Tenta criar a thread usando SCHED_FIFO e prioridade informada.
// Caso o ambiente nao permita politica de tempo real, cria uma
// thread normal para que o experimento ainda possa ser executado.
static int create_rt_thread(pthread_t *th,
                            void *(*fn)(void *),
                            void *arg,
                            int priority)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
    pthread_attr_setschedpolicy(&attr, SCHED_FIFO);

    struct sched_param sp = {0};
    sp.sched_priority = priority;

    pthread_attr_setschedparam(&attr, &sp);

    int rc = pthread_create(th, &attr, fn, arg);

    pthread_attr_destroy(&attr);

    if (rc == 0) {
        return 0;
    }

    fprintf(stderr,
            "[AVISO] pthread RT falhou (%s). Criando thread normal.\n",
            strerror(rc));

    return pthread_create(th, NULL, fn, arg);
}

// ============================================================
// MAIN
// ============================================================
// Uso:
//   sudo ./m3_servidor <cenario> <Cs_em_ms>
//
// Exemplos usados nos experimentos:
//   sudo ./m3_servidor 1 3   -> carga leve, Cs = 3 ms
//   sudo ./m3_servidor 2 3   -> rajada, Cs = 3 ms
//   sudo ./m3_servidor 2 5   -> rajada, Cs = 5 ms
//
// Ts foi mantido fixo em 10 ms nos experimentos.
// ============================================================

int main(int argc, char **argv)
{
    // Valores padrao caso nenhum argumento seja fornecido.
    int scenario = 1;
    double Cs_ms = 3.0;
    const double Ts_ms = 10.0;

    // Primeiro argumento: numero do cenario.
    if (argc >= 2) {
        scenario = atoi(argv[1]);
    }

    // Segundo argumento: orçamento Cs em milissegundos.
    if (argc >= 3) {
        Cs_ms = atof(argv[2]);
    }

    // Validacao dos parametros recebidos.
    if (scenario < 1 || scenario > 3) {
        fprintf(stderr, "Cenario deve ser 1, 2 ou 3.\n");
        return EXIT_FAILURE;
    }

    if (Cs_ms <= 0.0 || Cs_ms > Ts_ms) {
        fprintf(stderr, "Cs deve ser > 0 e <= Ts (10 ms).\n");
        return EXIT_FAILURE;
    }

    printf("============================================================\n");
    printf(" M3 - SERVIDOR PERIODICO PARA SORT_ACT\n");
    printf(" Esteira industrial - evolucao da M2\n");
    printf("============================================================\n");
    printf("Cenario = %d\n", scenario);
    printf("Ts      = %.2f ms\n", Ts_ms);
    printf("Cs      = %.2f ms\n", Cs_ms);
    printf("Us      = %.1f%%\n", 100.0 * (Cs_ms / Ts_ms));
    printf("SORT: C ~= 0,7 ms | D = 10 ms\n\n");

    // Parametros do servidor periodico.
    server_params_t server_params = {
        .period_ns = (long)(Ts_ms * NS_PER_MS),
        .budget_ns = (long)(Cs_ms * NS_PER_MS)
    };

    // ENC_SENSE: tarefa periodica de maior frequencia.
    periodic_params_t enc_params = {
        .name = "ENC_SENSE",
        .period_ns = ENC_T_NS,
        .deadline_ns = ENC_D_NS,
        .computation_us = ENC_C_US,
        .metrics = &enc_metrics
    };

    // SPD_CTRL: segunda tarefa periodica representativa.
    periodic_params_t ctrl_params = {
        .name = "SPD_CTRL",
        .period_ns = CTRL_T_NS,
        .deadline_ns = CTRL_D_NS,
        .computation_us = CTRL_C_US,
        .metrics = &ctrl_metrics
    };

    generator_params_t generator_params = {
        .scenario = scenario
    };

    pthread_t th_server;
    pthread_t th_enc;
    pthread_t th_ctrl;
    pthread_t th_generator;

    // Prioridades escolhidas:
    // ENC_SENSE > SPD_CTRL > SERVIDOR.
    // O objetivo e permitir que as tarefas periodicas mantenham
    // prioridade superior ao atendimento aperiódico.
    create_rt_thread(&th_enc,
                     periodic_thread,
                     &enc_params,
                     70);

    create_rt_thread(&th_ctrl,
                     periodic_thread,
                     &ctrl_params,
                     65);

    create_rt_thread(&th_server,
                     server_thread,
                     &server_params,
                     60);

    // O gerador representa a chegada externa dos eventos SORT.
    pthread_create(&th_generator,
                   NULL,
                   generator_thread,
                   &generator_params);

    // Primeiro aguardamos o fim da geracao das requisicoes e
    // depois o servidor esvaziar toda a fila.
    pthread_join(th_generator, NULL);
    pthread_join(th_server, NULL);

    // Com a fila vazia, encerramos as tarefas periodicas.
    stop_periodic = true;

    pthread_join(th_enc, NULL);
    pthread_join(th_ctrl, NULL);

    // ========================================================
    // RESULTADOS FINAIS DO EXPERIMENTO
    // ========================================================

    printf("\n============================================================\n");
    printf(" RESULTADOS\n");
    printf("============================================================\n");

    printf("ENC_SENSE | jobs=%lu | misses=%lu | WCRT_obs=%ld us\n",
           (unsigned long)enc_metrics.jobs,
           (unsigned long)enc_metrics.misses,
           (long)enc_metrics.max_response_us);

    printf("SPD_CTRL  | jobs=%lu | misses=%lu | WCRT_obs=%ld us\n",
           (unsigned long)ctrl_metrics.jobs,
           (unsigned long)ctrl_metrics.misses,
           (long)ctrl_metrics.max_response_us);

    printf("SORT_ACT  | gerados=%lu | atendidos=%lu | misses=%lu\n",
           (unsigned long)sort_metrics.generated,
           (unsigned long)sort_metrics.served,
           (unsigned long)sort_metrics.misses);

    printf("SORT_ACT  | espera_max=%ld us | resposta_max=%ld us\n",
           (long)sort_metrics.max_wait_us,
           (long)sort_metrics.max_response_us);

    printf("SERVIDOR  | estouros_orcamento=%lu | fila_max=%lu\n",
           (unsigned long)sort_metrics.budget_exhaustions,
           (unsigned long)sort_metrics.max_queue_len);

    printf("============================================================\n");

    return 0;
}
