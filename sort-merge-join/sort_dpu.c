// dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o sort_dpu sort_dpu.c
// use 24 tasklets (max)
// use 2 tasklets for test

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

#define STACK_SIZE 100

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host dpu_block_t bl;
uint32_t addr[NR_TASKLETS];
int rows[NR_TASKLETS];

void quick_sort(uint32_t addr, int row_num, int col_num, int key)
{
    int *pivot_arr = (int *)mem_alloc(col_num * sizeof(int));
    int *temp_i_arr = (int *)mem_alloc(col_num * sizeof(int));
    int *temp_j_arr = (int *)mem_alloc(col_num * sizeof(int));

    int *stack = (int *)mem_alloc(STACK_SIZE * sizeof(int));
    int top = -1;

    stack[++top] = 0;
    stack[++top] = row_num - 1;

    while (top >= 0)
    {
        int pRight = stack[top--];
        int pLeft = stack[top--];

        int offset = ((pRight + pLeft) / 2) * col_num * sizeof(int);
        mram_read((__mram_ptr void const *)(addr + offset), pivot_arr, col_num * sizeof(int));
        int pivot = pivot_arr[key];

        int i = pLeft;
        int j = pRight;

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

    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(int);
    addr[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    /* do quick sort */

    quick_sort(addr[tasklet_id], rows[tasklet_id], col_num, JOIN_KEY);
    barrier_wait(&my_barrier);

    /* do merge sort */

    int running = NR_TASKLETS;
    // int running = 2;
    int step = 2;

    int *first_row = (int *)mem_alloc(col_num * sizeof(int));
    int *second_row = (int *)mem_alloc(col_num * sizeof(int));
    int *tmp_row = (int *)mem_alloc(col_num * sizeof(int));
    int *save_row = (int *)mem_alloc(col_num * sizeof(int));

    while (running > 1)
    {
        if(tasklet_id == 0) running = (running + 1) / 2;
        if (tasklet_id % step == 0)
        {
            int first_cnt = 0;
            int trg = tasklet_id + step / 2;

            if (trg < NR_TASKLETS)
            {
                uint32_t first_addr = addr[tasklet_id];
                uint32_t second_addr = addr[trg];

                while (first_cnt < rows[tasklet_id])
                {
                    mram_read((__mram_ptr void *)(first_addr + first_cnt * col_num * sizeof(int)), first_row, col_num * sizeof(int));
                    mram_read((__mram_ptr void *)(second_addr), second_row, col_num * sizeof(int));

                    if (first_row[JOIN_KEY] > second_row[JOIN_KEY])
                    {
                        // exchange
                        mram_write(first_row, (__mram_ptr void *)second_addr, col_num * sizeof(int));
                        mram_write(second_row, (__mram_ptr void *)(first_addr + first_cnt * col_num * sizeof(int)), col_num * sizeof(int));

                        // re-sort in second
                        int change_idx = 1;

                        mram_read((__mram_ptr void *)(second_addr), save_row, col_num * sizeof(int));
                        mram_read((__mram_ptr void *)(second_addr + change_idx * col_num * sizeof(int)), tmp_row, col_num * sizeof(int));

                        int next_val = tmp_row[JOIN_KEY];
                        while (next_val < save_row[JOIN_KEY])
                        {
                            mram_write(tmp_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(int)), col_num * sizeof(int));
                            change_idx++;
                            mram_read((__mram_ptr void *)(second_addr + change_idx * col_num * sizeof(int)), tmp_row, col_num * sizeof(int));
                            next_val = tmp_row[JOIN_KEY];

                            if (change_idx == rows[trg])
                                break;
                        }

                        mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(int)), col_num * sizeof(int));
                    }

                    first_cnt++;
                }

                rows[tasklet_id] += rows[trg];
            }

            step *= 2;
        }
        barrier_wait(&my_barrier);
    }

#ifdef DEBUG
    mutex_lock(my_mutex);
    printf("Sort Tasklet %d: %d\n", tasklet_id, rows[tasklet_id]);
    mutex_unlock(my_mutex);
#endif
    mem_reset();

    return 0;
}