// dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o task task.c

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

#define UNDEFINED_VAL (-1)
int shared_var = UNDEFINED_VAL;

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host dpu_block_t bl;

int select_row[NR_TASKLETS];
bool check[NR_TASKLETS];

int sum_array(int *arr, int size)
{
    int sum = 0;
    for (int i = 0; i < size; i++)
    {
        sum += arr[i];
    }
    return sum;
}

bool is_all_true(bool *arr, int size)
{
    for (int i = 0; i < size; i++)
    {
        if (!arr[i])
        {
            return false;
        }
    }
    return true;
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

    // -------------------- Select --------------------

    for (int i = 0; i < row_per_tasklet; i++)
    {
        mram_read((__mram_ptr void const *)(mram_base_addr + i * col_num * sizeof(int)), tasklet_row_array, col_num * sizeof(int));
        if (tasklet_row_array[SELECT_COL] > SELECT_VAL)
        {
            cnt++;
        }
    }
    select_row[tasklet_id] = cnt;
    barrier_wait(&my_barrier);

    while (is_all_true(check, NR_TASKLETS))
    {
        if (!check[tasklet_id] && check[tasklet_id - 1])
        {
            int shift = sum_array(select_row, tasklet_id);
            cnt = 0;
            uint32_t mram_shift_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + shift * col_num * sizeof(int);
            for (int i = 0; i < row_per_tasklet; i++)
            {
                mram_read((__mram_ptr void const *)(mram_base_addr + i * col_num * sizeof(int)), tasklet_row_array, col_num * sizeof(int));
                if (tasklet_row_array[SELECT_COL] > SELECT_VAL)
                {
                    int offset = cnt * col_num;
                    mram_write(tasklet_row_array, (__mram_ptr void *)(mram_shift_addr + offset * sizeof(int)), col_num * sizeof(int));
                    cnt++;
                }
            }
            check[tasklet_id] = true;
        }
    }
    barrier_wait(&my_barrier);

    // -------------------- Transfer --------------------

#ifdef DEBUG
    mutex_lock(my_mutex);
    printf("Select Tasklet %d: %d\n", tasklet_id, select_row[tasklet_id]);
    mutex_unlock(my_mutex);
#endif

    if (tasklet_id == NR_TASKLETS - 1)
    {
        bl.col_num = col_num;
        bl.row_num = sum_array(select_row, NR_TASKLETS);
    }

    mem_reset();

    return 0;
}