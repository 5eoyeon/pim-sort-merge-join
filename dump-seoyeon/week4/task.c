// dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o task task.c

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>

#define NR_TASKLETS 2
#define MAX_COL 10
#define MAX_ROW 10

#define UNDEFINED_VAL (-1)
int shared_var = UNDEFINED_VAL;

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host int col_num;
__host int row_num;
__mram_noinit int test_array[MAX_ROW * MAX_COL];
__mram_noinit int tasklet_array[MAX_ROW * MAX_COL];

int main() {
    int chunk_size = (row_num / NR_TASKLETS)*col_num;
    unsigned int tasklet_id = me();
    int start = tasklet_id * chunk_size;
    __mram_ptr int *tasklet_test_array = &test_array[start];

    if(tasklet_id == NR_TASKLETS-1) chunk_size += (col_num*row_num) - NR_TASKLETS*chunk_size;

    mutex_lock(my_mutex);
    printf("Tasklet %d is running\n", tasklet_id);
    for(int i = 0; i < chunk_size / col_num; i++) {
        for(int j = 0; j < col_num; j++) printf("%d ", *(tasklet_test_array + i*col_num + j));
        printf("\n");
    }
    printf("\n");

    mutex_unlock(my_mutex);
    barrier_wait(&my_barrier);

    return 0;
}