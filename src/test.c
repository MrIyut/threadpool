// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>

#include "os_graph.h"
#include "os_threadpool.h"
#include "log/log.h"
#include "utils.h"

#define NUM_THREADS		4

static int sum;
static os_graph_t *graph;
static os_threadpool_t *tp;

/* graph synchronization mechanisms. */
pthread_mutex_t work_mutex;
pthread_mutex_t neighbours_mutex;
int *node_numbers;


static void process_node(void *nodeIndex);
static void process_neighbours(void *nodeIndex) {
	unsigned int idx = *(unsigned int *)nodeIndex;
	os_node_t *node = graph->nodes[idx];

	for (unsigned int i = 0; i < node->num_neighbours; i++) {
		pthread_mutex_lock(&neighbours_mutex);
		if (graph->visited[node->neighbours[i]] == NOT_VISITED) {
			graph->visited[node->neighbours[i]] = PROCESSING;
			make_task(tp, process_node, (void *) (node_numbers + node->neighbours[i]), NULL);
		}
		pthread_mutex_unlock(&neighbours_mutex);
	}
}

static void process_node(void *nodeIndex) {
	os_node_t *node;
	unsigned int idx = *(unsigned int *)nodeIndex;

	node = graph->nodes[idx];
	if (graph->visited[idx] == DONE)
		return;

	pthread_mutex_lock(&work_mutex);
	sum += node->info;
	graph->visited[idx] = DONE;
	make_task(tp, process_neighbours, (void *) (node_numbers + idx), NULL);
	pthread_mutex_unlock(&work_mutex);
}

int main(int argc, char *argv[]) {
	FILE *input_file;
	if (argc != 2) {
		fprintf(stderr, "Usage: %s input_file\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	input_file = fopen(argv[1], "r");
	DIE(input_file == NULL, "fopen");

	graph = create_graph_from_file(input_file);
	node_numbers = calloc(graph->num_nodes, sizeof(int));
	for (unsigned int i = 0; i < graph->num_nodes; i++)
		node_numbers[i] = i;
	pthread_mutex_init(&work_mutex, NULL);
	pthread_mutex_init(&neighbours_mutex, NULL);

	tp = create_threadpool(NUM_THREADS);

	make_task(tp, process_node, (void *) node_numbers, NULL);
	wait_for_completion(tp);
	destroy_threadpool(tp);

	printf("%d", sum);

	pthread_mutex_destroy(&work_mutex);
	pthread_mutex_destroy(&neighbours_mutex);
	free(node_numbers);
	return 0;
}
