// dpu-upmem-dpurte-clang -DNR_TASKLETS=10 -o lock lock.c

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>

#define NR_TASKLETS 10
#define CHUNK_SIZE 50

#define UNDEFINED_VAL (-1)
int shared_var = UNDEFINED_VAL;

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__mram_noinit int test_array[1000];

int main() {
    unsigned int tasklet_id = me();

    int start = tasklet_id * CHUNK_SIZE;
    int end = start + CHUNK_SIZE;
    for (int i = start; i < end; i++) test_array[i] *= test_array[i];

    mutex_lock(my_mutex);
    
    printf("Tasklet %d is running\n", tasklet_id);
    for(int i = start; i < end; i++) printf("%d ", test_array[i]);
    printf("\n");

    mutex_unlock(my_mutex);

    barrier_wait(&my_barrier);
    for(int i = start + 1; i < end; i++) test_array[i] += test_array[i - 1];

    return 0;
}