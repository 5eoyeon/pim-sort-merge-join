#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"
#include "user.h"

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host dpu_block_t bl1;
__host dpu_block_t bl2;

uint32_t addr[NR_TASKLETS];
T rows[NR_TASKLETS];
int used_idx[NR_TASKLETS];
int used_rows[NR_TASKLETS];

// Find matched index or lower bound index
int binary_search(uint32_t base_addr, int col_num, int row_num, int target, int join_key)
{
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
    /* **************** */
    /*     Allocate     */
    /* **************** */

    // Initialize variables
    unsigned int tasklet_id = me();
    int col_num = bl1.col_num;
    int row_num1 = bl1.row_num;
    int row_num2 = bl2.row_num;
    int one_row_size = col_num * sizeof(T);
    unsigned int join_key = bl1.table_num == 0 ? JOIN_KEY1 : JOIN_KEY2;
    uint32_t mram_base_addr_dpu2 = (uint32_t)DPU_MRAM_HEAP_POINTER + (row_num1 + row_num2) * one_row_size;

    // Calculate the chunk size and offset from the base address
    int row_per_tasklet = row_num1 / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num;
    int start = tasklet_id * chunk_size;
    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num1 - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    // Initialize the start address
    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(T);

    // Save the address and the number of rows
    addr[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    /* ********************* */
    /*     Binary search     */
    /* ********************* */

    // Load the last row
    T *last_row = (T *)mem_alloc(one_row_size);
    mram_read((__mram_ptr void *)(mram_base_addr + (row_per_tasklet - 1) * one_row_size), last_row, one_row_size);

    // Find the index
    if (tasklet_id < NR_TASKLETS - 1)
    {
        used_idx[tasklet_id] = binary_search(mram_base_addr_dpu2, col_num, row_num2, last_row[join_key], join_key);
    }
    else
    {
        used_idx[tasklet_id] = row_num2 - 1;
    }

    // Barrier
    barrier_wait(&my_barrier);

    /* *********************** */
    /*     Merge with dpus     */
    /* *********************** */

    // Initialize the index
    int start_idx = tasklet_id == 0 ? 0 : used_idx[tasklet_id - 1] + 1;
    int end_idx = used_idx[tasklet_id];

    // Initialize local caches
    T *first_row = (T *)mem_alloc(one_row_size);
    T *second_row = (T *)mem_alloc(one_row_size);
    T *tmp_row = (T *)mem_alloc(one_row_size);
    T *save_row = (T *)mem_alloc(one_row_size);

    // Save the number of rows
    used_rows[tasklet_id] = end_idx - start_idx + 1;

    // Initialize the counter and addresses
    int cur_cnt = 0;
    uint32_t first_addr = addr[tasklet_id];
    uint32_t second_addr = mram_base_addr_dpu2 + start_idx * one_row_size;

    while (cur_cnt < rows[tasklet_id])
    {
        // Load the rows
        mram_read((__mram_ptr void *)(first_addr + cur_cnt * one_row_size), first_row, one_row_size);
        mram_read((__mram_ptr void *)(second_addr), second_row, one_row_size);

        // Compare
        if (first_row[join_key] > second_row[join_key])
        {
            // Exchange
            mram_write(first_row, (__mram_ptr void *)second_addr, one_row_size);
            mram_write(second_row, (__mram_ptr void *)(first_addr + cur_cnt * one_row_size), one_row_size);

            // Re-sort in second
            int change_idx = 1;

            mram_read((__mram_ptr void *)(second_addr), save_row, one_row_size);
            mram_read((__mram_ptr void *)(second_addr + change_idx * one_row_size), tmp_row, one_row_size);

            int next_val = tmp_row[join_key];
            while (next_val < save_row[join_key])
            {
                mram_write(tmp_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * one_row_size), one_row_size);
                change_idx++;

                mram_read((__mram_ptr void *)(second_addr + change_idx * one_row_size), tmp_row, one_row_size);
                next_val = tmp_row[join_key];

                if (change_idx == used_rows[tasklet_id])
                {
                    break;
                }
            }

            mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * one_row_size), one_row_size);
        }

        // Increment the counter
        cur_cnt++;
    }

    // Barrier
    barrier_wait(&my_barrier);

    /* *********************************** */
    /*     Re-sort (dpu-i & dpu-(i+1))     */
    /* *********************************** */

    // Initialize the target address
    uint32_t target_addr;
    if (tasklet_id == 0)
    {
        target_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + (rows[0] - 1) * one_row_size;
    }
    else
    {
        target_addr = addr[tasklet_id] + (used_idx[tasklet_id - 1] + rows[tasklet_id]) * one_row_size;
    }

    // Read and write rows in reverse order
    for (int i = NR_TASKLETS - 1; i >= 0; i--)
    {
        // Barrier
        barrier_wait(&my_barrier);

        if (tasklet_id == i)
        {
            for (int r = rows[tasklet_id] - 1; r >= 0; r--)
            {
                mram_read((__mram_ptr void *)(addr[tasklet_id] + r * one_row_size), tmp_row, one_row_size);
                mram_write(tmp_row, (__mram_ptr void *)(target_addr), one_row_size);
                target_addr -= one_row_size;
            }
        }

        // Barrier
        barrier_wait(&my_barrier);
    }

    // Initialize the insert address
    uint32_t insert_addr = target_addr + (rows[tasklet_id] + 1) * one_row_size;

    // Insert the rows
    for (int r = 0; r < used_rows[tasklet_id]; r++)
    {
        mram_read((__mram_ptr void *)(second_addr + r * one_row_size), tmp_row, one_row_size);
        mram_write(tmp_row, (__mram_ptr void *)(insert_addr + r * one_row_size), one_row_size);
    }

    // Reset the heap
    mem_reset();

    return 0;
}