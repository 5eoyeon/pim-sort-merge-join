// dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o alloc_array alloc_array.c

#include <stdio.h>
#include <barrier.h>
#include <defs.h>
#include <stdint.h>

#define NR_TASKLETS 2
#define CHUNK_SIZE 10

BARRIER_INIT(my_barrier, NR_TASKLETS);

__mram_noinit int test_array[20];

int main() {
    unsigned int tasklet_id = me();

    int start = tasklet_id * CHUNK_SIZE;
    int end = start + CHUNK_SIZE;
    for (int i = start; i < end; i++) test_array[i] *= test_array[i];

    printf("Tasklet %d is running\n", tasklet_id);
    for(int i = start; i < end; i++) printf("%d ", test_array[i]);
    printf("\n");

    barrier_wait(&my_barrier);

    return 0;
}