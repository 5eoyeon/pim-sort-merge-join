// dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o sort sort.c

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

__host dpu_block_t bl;

void quick_sort(uint32_t addr, int row_num, int col_num, int key)
{
    if (row_num <= 1)
        return;

    int *tasklet_row_arr = (int *)mem_alloc(col_num * sizeof(int));
    int offset = (row_num / 2) * col_num * sizeof(int);
    mram_read((__mram_ptr void const *)(addr + offset), tasklet_row_arr, col_num * sizeof(int));

    int pivot = tasklet_row_arr[key];
    int i = 0;
    int j = row_num - 1;
    int *temp_i_arr = (int *)mem_alloc(col_num * sizeof(int));
    int *temp_j_arr = (int *)mem_alloc(col_num * sizeof(int));

    while (i <= j)
    {
        mram_read((__mram_ptr void const *)(addr + i * col_num * sizeof(int)), temp_i_arr, col_num * sizeof(int));
        mram_read((__mram_ptr void const *)(addr + j * col_num * sizeof(int)), temp_j_arr, col_num * sizeof(int));

        while (temp_i_arr[key] < pivot && i <= j)
        {
            i++;
            mram_read((__mram_ptr void const *)(addr + i * col_num * sizeof(int)), temp_i_arr, col_num * sizeof(int));
        }

        while (temp_j_arr[key] > pivot && i <= j)
        {
            j--;
            mram_read((__mram_ptr void const *)(addr + j * col_num * sizeof(int)), temp_j_arr, col_num * sizeof(int));
        }

        if (i <= j)
        {
            mram_write(temp_i_arr, (__mram_ptr void *)(addr + j * col_num * sizeof(int)), col_num * sizeof(int));
            mram_write(temp_j_arr, (__mram_ptr void *)(addr + i * col_num * sizeof(int)), col_num * sizeof(int));

            i++;
            j--;
        }
    }

    quick_sort(addr, j + 1, col_num, key);
    quick_sort(addr + i * col_num * sizeof(int), row_num - i, col_num, key);
}

int main()
{
    // -------------------- Allocate --------------------

    unsigned int tasklet_id = me();
    int col_num = bl.col_num;
    int row_num = bl.row_num;

    int row_per_tasklet = row_num / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num;
    int start = tasklet_id * chunk_size;
    int cnt = 0;

    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    int *tasklet_row_array = (int *)mem_alloc(col_num * sizeof(int));
    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(int);

    // -------------------- Sort --------------------

    quick_sort(mram_base_addr, row_per_tasklet, col_num, JOIN_KEY);
    barrier_wait(&my_barrier);

#ifdef DEBUG
    printf("Sort Tasklet %d done\n", tasklet_id);
#endif
    mem_reset();
    return 0;
}