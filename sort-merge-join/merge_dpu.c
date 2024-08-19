// dpu-upmem-dpurte-clang -DNR_TASKLETS=1 -o merge_dpu merge_dpu.c

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

BARRIER_INIT(my_barrier, NR_TASKLETS);

__host int col_num;
__host int row_num;
__host int merge_array[MAX_ROW * MAX_COL];
__mram_noinit int result_array;

int main()
{
    int* merge_array0 = merge_array;
    int* merge_array1 = merge_array + row_num/2 * col_num;

    // for (int r = 0; r < row_num; r++)
    // {
    //     for (int c = 0; c < col_num; c++)
    //     {
    //         printf("%d ", *(merge_array + r * col_num + c));
    //     }
    //     printf("\n");
    // }

    int row_per_tasklet = row_num / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num;
    unsigned int tasklet_id = me();
    int start = tasklet_id * chunk_size;
    __mram_ptr int *tasklet_test_array = &merge_array[start];

    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    // use `merge_in_asc`
    barrier_wait(&my_barrier);

    return 0;
}