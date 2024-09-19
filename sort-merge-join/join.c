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
MUTEX_INIT(my_mutex);

__host dpu_block_t bl1;
__host dpu_block_t bl2;
uint32_t addr[NR_TASKLETS];
int rows[NR_TASKLETS];
int used_idx[NR_TASKLETS];
int used_rows[NR_TASKLETS];

int binary_search(uint32_t base_addr, int col_num, int row_num, int target)
{
    int left = 0;
    int right = row_num - 1;
    int idx = -1;

    int *mid_row = (int *)mem_alloc(col_num * sizeof(int));
    while (left <= right)
    {
        int mid = (left + right) / 2;

        mram_read((__mram_ptr void *)(base_addr + mid * col_num * sizeof(int)), mid_row, col_num * sizeof(int));

        if (mid_row[JOIN_KEY] == target)
            return mid;
        else if (mid_row[JOIN_KEY] < target)
        {
            idx = mid;
            left = mid + 1;
        }
        else
            right = mid - 1;
    }

    return idx;
}

int main()
{
    int col_num1 = bl1.col_num;
    int col_num2 = bl2.col_num;
    int row_num1 = bl1.row_num;
    int row_num2 = bl2.row_num;
    uint32_t mram_base_addr_dpu2 = (uint32_t)DPU_MRAM_HEAP_POINTER + row_num1 * col_num1 * sizeof(int);

    unsigned int tasklet_id = me();

    int row_per_tasklet = row_num1 / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num1;
    int start = tasklet_id * chunk_size;
    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num1 - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num1;
    }

    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(int);
    addr[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    /* **************** */
    /* do binary search */
    /* **************** */
    int *last_row = (int *)mem_alloc(col_num1 * sizeof(int));
    mram_read((__mram_ptr void *)(mram_base_addr + (row_per_tasklet - 1) * col_num1 * sizeof(int)), last_row, col_num1 * sizeof(int));
    if (tasklet_id < NR_TASKLETS - 1)
    {
        used_idx[tasklet_id] = binary_search(mram_base_addr_dpu2, col_num2, row_num2, last_row[JOIN_KEY]);
    }
    else
    {
        used_idx[tasklet_id] = row_num2 - 1;
    }

    barrier_wait(&my_barrier);

    /* ******* */
    /* do join */
    /* ******* */
    int start_idx;
    if (tasklet_id == 0)
        start_idx = 0;
    else
        start_idx = used_idx[tasklet_id - 1] + 1;
    int end_idx = used_idx[tasklet_id];

    int *first_row = (int *)mem_alloc(col_num1 * sizeof(int));
    int *second_row = (int *)mem_alloc(col_num2 * sizeof(int));
    int *merge_row = (int *)mem_alloc((col_num1 + col_num2 - 1) * sizeof(int));

    used_rows[tasklet_id] = end_idx - start_idx + 1;

    int cur_idx1 = 0;
    int cur_idx2 = 0;
    uint32_t first_addr = addr[tasklet_id];
    uint32_t second_addr = mram_base_addr_dpu2 + start_idx * col_num2 * sizeof(int);

    int res_idx = 0;

    mram_read((__mram_ptr void *)(first_addr + cur_idx1 * col_num1 * sizeof(int)), first_row, col_num1 * sizeof(int));
    mram_read((__mram_ptr void *)(second_addr + cur_idx2 * col_num2 * sizeof(int)), second_row, col_num2 * sizeof(int));

    while (cur_idx1 < rows[tasklet_id] && cur_idx2 < used_rows[tasklet_id])
    {
        if (first_row[JOIN_KEY] == second_row[JOIN_KEY])
        {
            int cur_col = 0;
            for (int c = 0; c < col_num1; c++)
            {
                merge_row[cur_col] = first_row[c];
                cur_col++;
            }
            for (int c = 0; c < col_num2; c++)
            {
                if (c == JOIN_KEY)
                    continue;
                merge_row[cur_col] = second_row[c];
                cur_col++;
            }

            mram_write(merge_row, (__mram_ptr void *)addr, (col_num1 + col_num2 - 1) * sizeof(int));

            res_idx++;
            cur_idx1++;
            cur_idx2++;
        }
        else if (first_row[JOIN_KEY] < second_row[JOIN_KEY])
        {
            cur_idx1++;
        }
        else
        {
            cur_idx2++;
        }
    }

    return 0;
}