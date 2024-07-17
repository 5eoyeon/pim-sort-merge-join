// dpu - upmem - dpurte - clang - DNR_TASKLETS = 2 - o task task.c

#include <stdio.h>
#include <barrier.h>
#include <defs.h>
#include <stdint.h>
#include <string.h>

#define NR_TASKLETS 2

BARRIER_INIT(my_barrier, NR_TASKLETS);

__mram_noinit int test_array[100];
__mram_noinit uint64_t col_num;
__mram_noinit uint64_t row_num;

int main()
{
    unsigned int tasklet_id = me();
    printf("col_num : %d, row_num : %d\n", col_num, row_num);

    int rows_to_sort = row_num / NR_TASKLETS;
    int size = (tasklet_id == NR_TASKLETS - 1) ? (row_num - (NR_TASKLETS - 1) * rows_to_sort) * col_num : rows_to_sort * col_num;

    int start = tasklet_id * size;
    int end = start + size;

    for (int i = start; i < end; i++)
        test_array[i] *= test_array[i];

    printf("Tasklet %d is running\n", tasklet_id);
    for (int i = start; i < end; i++)
        printf("%d ", test_array[i]);
    printf("\n");

    barrier_wait(&my_barrier);

    return 0;
}