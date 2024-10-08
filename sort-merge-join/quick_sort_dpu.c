#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

#define STACK_SIZE 1500

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host dpu_block_t bl;
uint32_t addr[NR_TASKLETS];
T rows[NR_TASKLETS];

void quick_sort(uint32_t addr, int row_num, int col_num, int key)
{
    T *pivot_arr = (T *)mem_alloc(col_num * sizeof(T));
    T *temp_i_arr = (T *)mem_alloc(col_num * sizeof(T));
    T *temp_j_arr = (T *)mem_alloc(col_num * sizeof(T));

    int *stack = (int *)mem_alloc(STACK_SIZE * sizeof(int));
    int top = -1;

    stack[++top] = 0;
    stack[++top] = row_num - 1;

    while (top >= 0)
    {
        int pRight = stack[top--];
        int pLeft = stack[top--];

        int offset = ((pRight + pLeft) / 2) * col_num * sizeof(T);
        mram_read((__mram_ptr void const *)(addr + offset), pivot_arr, col_num * sizeof(T));
        int pivot = pivot_arr[key];

        int i = pLeft;
        int j = pRight;

        while (i <= j)
        {
            mram_read((__mram_ptr void const *)(addr + i * col_num * sizeof(T)), temp_i_arr, col_num * sizeof(T));
            mram_read((__mram_ptr void const *)(addr + j * col_num * sizeof(T)), temp_j_arr, col_num * sizeof(T));

            while (temp_i_arr[key] < pivot && i <= j)
            {
                i++;
                mram_read((__mram_ptr void const *)(addr + i * col_num * sizeof(T)), temp_i_arr, col_num * sizeof(T));
            }

            while (temp_j_arr[key] > pivot && i <= j)
            {
                j--;
                mram_read((__mram_ptr void const *)(addr + j * col_num * sizeof(T)), temp_j_arr, col_num * sizeof(T));
            }

            if (i <= j)
            {
                mram_write(temp_i_arr, (__mram_ptr void *)(addr + j * col_num * sizeof(T)), col_num * sizeof(T));
                mram_write(temp_j_arr, (__mram_ptr void *)(addr + i * col_num * sizeof(T)), col_num * sizeof(T));

                i++;
                j--;
            }
        }

        if (i < pRight)
        {
            stack[++top] = i;
            stack[++top] = pRight;
        }
        if (pLeft < j)
        {
            stack[++top] = pLeft;
            stack[++top] = j;
        }
    }
}

int main()
{
    int col_num = bl.col_num;
    int row_num = bl.row_num;

    unsigned int tasklet_id = me();

    int row_per_tasklet = row_num / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num;
    int start = tasklet_id * chunk_size;
    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(T);
    addr[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    int join_key;
    if (bl.table_num == 0) {
        join_key = JOIN_KEY1;
    } else {
        join_key = JOIN_KEY2;
    }

    /* do quick sort */

    quick_sort(addr[tasklet_id], rows[tasklet_id], col_num, join_key);
    barrier_wait(&my_barrier);

#ifdef DEBUG
    mutex_lock(my_mutex);
    printf("Quick-Sort Tasklet %d: %d\n", tasklet_id, rows[tasklet_id]);
    mutex_unlock(my_mutex);
#endif
    mem_reset();

    return 0;
}