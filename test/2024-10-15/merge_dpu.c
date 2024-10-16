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
T rows[NR_TASKLETS];
int used_idx[NR_TASKLETS];
int used_rows[NR_TASKLETS];

int binary_search(uint32_t base_addr, int col_num, int row_num, int target, int join_key)
{ // find matched index or lower bound index
    int left = 0;
    int right = row_num - 1;
    int idx = -1;

    T *mid_row = (T *)mem_alloc(col_num * sizeof(T));
    while (left <= right)
    {
        int mid = (left + right) / 2;

        mram_read((__mram_ptr void *)(base_addr + mid * col_num * sizeof(T)), mid_row, col_num * sizeof(T));

        if (mid_row[join_key] == target)
        {
            return mid;
        }
        else if (mid_row[join_key] < target)
        {
            idx = mid;
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }

    return idx;
}

int main()
{
    int col_num = bl1.col_num;
    int row_num1 = bl1.row_num;
    int row_num2 = bl2.row_num;
    uint32_t mram_base_addr_dpu2 = (uint32_t)DPU_MRAM_HEAP_POINTER + (row_num1 + row_num2) * col_num * sizeof(T);

    unsigned int tasklet_id = me();

    int row_per_tasklet = row_num1 / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num;
    int start = tasklet_id * chunk_size;
    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num1 - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(T);
    addr[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    int join_key;
    if (bl1.table_num == 0) {
        join_key = JOIN_KEY1;
    } else {
        join_key = JOIN_KEY2;
    }

    /* **************** */
    /* do binary search */
    /* **************** */
    T *last_row = (T *)mem_alloc(col_num * sizeof(T));
    mram_read((__mram_ptr void *)(mram_base_addr + (row_per_tasklet - 1) * col_num * sizeof(T)), last_row, col_num * sizeof(T));
    if (tasklet_id < NR_TASKLETS - 1)
    {
        used_idx[tasklet_id] = binary_search(mram_base_addr_dpu2, col_num, row_num2, last_row[join_key], join_key);
    }
    else
    {
        used_idx[tasklet_id] = row_num2 - 1;
    }

    barrier_wait(&my_barrier);

    /* ************* */
    /* do merge sort */
    /* ************* */
    int start_idx;
    if (tasklet_id == 0)
        start_idx = 0;
    else
        start_idx = used_idx[tasklet_id - 1] + 1;
    int end_idx = used_idx[tasklet_id];

    T *first_row = (T *)mem_alloc(col_num * sizeof(T));
    T *second_row = (T *)mem_alloc(col_num * sizeof(T));
    T *tmp_row = (T *)mem_alloc(col_num * sizeof(T));
    T *save_row = (T *)mem_alloc(col_num * sizeof(T));

    used_rows[tasklet_id] = end_idx - start_idx + 1;

    int cur_cnt = 0;
    uint32_t first_addr = addr[tasklet_id];
    uint32_t second_addr = mram_base_addr_dpu2 + start_idx * col_num * sizeof(T);

    while (cur_cnt < rows[tasklet_id])
    {
        mram_read((__mram_ptr void *)(first_addr + cur_cnt * col_num * sizeof(T)), first_row, col_num * sizeof(T));
        mram_read((__mram_ptr void *)(second_addr), second_row, col_num * sizeof(T));

        if (first_row[join_key] > second_row[join_key])
        {
            // exchange
            mram_write(first_row, (__mram_ptr void *)second_addr, col_num * sizeof(T));
            mram_write(second_row, (__mram_ptr void *)(first_addr + cur_cnt * col_num * sizeof(T)), col_num * sizeof(T));

            // re-sort in second
            int change_idx = 1;

            mram_read((__mram_ptr void *)(second_addr), save_row, col_num * sizeof(T));
            mram_read((__mram_ptr void *)(second_addr + change_idx * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));

            int next_val = tmp_row[join_key];
            while (next_val < save_row[join_key])
            {
                mram_write(tmp_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(T)), col_num * sizeof(T));
                change_idx++;
              
                mram_read((__mram_ptr void *)(second_addr + change_idx * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));
                next_val = tmp_row[join_key];

                if (change_idx == used_rows[tasklet_id])
                    break;
            }

            mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(T)), col_num * sizeof(T));
        }

        cur_cnt++;
    }

    barrier_wait(&my_barrier);

    /* *************************** */
    /* re-sort (dpu-i & dpu-(i+1)) */
    /* *************************** */
    uint32_t target_addr;
    if (tasklet_id == 0)
        target_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + (rows[0] - 1) * col_num * sizeof(T);
    else
        target_addr = addr[tasklet_id] + (used_idx[tasklet_id - 1] + rows[tasklet_id]) * col_num * sizeof(T); // target_addr = mram_base_addr + (used_idx[tasklet_id - 1] + 1) * col_num * sizeof(T);

    for (int i = NR_TASKLETS - 1; i >= 0; i--)
    {
        barrier_wait(&my_barrier);

        if (tasklet_id == i)
        {
            for (int r = rows[tasklet_id] - 1; r >= 0; r--)
            {
                mram_read((__mram_ptr void *)(addr[tasklet_id] + r * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));
                mram_write(tmp_row, (__mram_ptr void *)(target_addr), col_num * sizeof(T));
                target_addr -= col_num * sizeof(T);
            }
        }

        barrier_wait(&my_barrier);
    }

    uint32_t insert_addr;
    insert_addr = target_addr + (rows[tasklet_id] + 1) * col_num * sizeof(T);

    for (int r = 0; r < used_rows[tasklet_id]; r++)
    {
        mram_read((__mram_ptr void *)(second_addr + r * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));
        mram_write(tmp_row, (__mram_ptr void *)(insert_addr + r * col_num * sizeof(T)), col_num * sizeof(T));
    }

    mem_reset();

    return 0;
}