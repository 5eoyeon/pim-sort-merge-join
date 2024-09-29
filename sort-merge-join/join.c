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
int joined_rows[NR_TASKLETS];
__host int joined_row;

int binary_search(uint32_t base_addr, int col_num, int row_num, T target)
{
    int left = 0;
    int right = row_num - 1;
    int idx = -1;

    T *mid_row = (T *)mem_alloc(col_num * sizeof(T));
    while (left <= right)
    {
        int mid = (left + right) / 2;

        mram_read((__mram_ptr void *)(base_addr + mid * col_num * sizeof(T)), mid_row, col_num * sizeof(T));

        if (mid_row[JOIN_KEY2] == target)
            return mid;
        else if (mid_row[JOIN_KEY2] < target)
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
    uint32_t mram_base_addr_dpu2 = (uint32_t)DPU_MRAM_HEAP_POINTER + row_num1 * col_num1 * sizeof(T);

    unsigned int tasklet_id = me();

    int row_per_tasklet = row_num1 / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num1;
    int start = tasklet_id * chunk_size;
    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num1 - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num1;
    }

    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(T);
    addr[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    /* **************** */
    /* do binary search */
    /* **************** */
    T *last_row = (T *)mem_alloc(col_num1 * sizeof(T));
    mram_read((__mram_ptr void *)(mram_base_addr + (row_per_tasklet - 1) * col_num1 * sizeof(T)), last_row, col_num1 * sizeof(T));
    if (tasklet_id < NR_TASKLETS - 1)
    {
        used_idx[tasklet_id] = binary_search(mram_base_addr_dpu2, col_num2, row_num2, last_row[JOIN_KEY1]);
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

    int total_col = col_num1 + col_num2 - 1;

    T *first_row = (T *)mem_alloc(col_num1 * sizeof(T));
    T *second_row = (T *)mem_alloc(col_num2 * sizeof(T));
    T *merge_row = (T *)mem_alloc(total_col * sizeof(T));

    used_rows[tasklet_id] = end_idx - start_idx + 1;

    int cur_idx1 = 0;
    int cur_idx2 = 0;
    uint32_t first_addr = addr[tasklet_id];
    uint32_t second_addr = mram_base_addr_dpu2 + start_idx * col_num2 * sizeof(T);
    uint32_t res_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + (row_num1 * col_num1 + row_num2 * col_num2) * sizeof(T);

    int res_idx = 0;
    int min_row = (row_num1 < row_num2) ? row_num1 : row_num2;
    int *selected_1 = (int *)mem_alloc(min_row * sizeof(int));
    int *selected_2 = (int *)mem_alloc(min_row * sizeof(int));

    mram_read((__mram_ptr void *)(first_addr + cur_idx1 * col_num1 * sizeof(T)), first_row, col_num1 * sizeof(T));
    mram_read((__mram_ptr void *)(second_addr + cur_idx2 * col_num2 * sizeof(T)), second_row, col_num2 * sizeof(T));
    
    int cur_row_idx = 0;
    while (cur_idx1 < rows[tasklet_id] && cur_idx2 < used_rows[tasklet_id])
    {
        if (first_row[JOIN_KEY1] == second_row[JOIN_KEY2])
        {
            printf("check!!! %d %d\n", cur_idx1, cur_idx2);
            selected_1[cur_row_idx] = cur_idx1;
            selected_2[cur_row_idx] = cur_idx2;

            cur_idx1++;
            cur_idx2++;
            cur_row_idx++;
        }
        else if (first_row[JOIN_KEY1] < second_row[JOIN_KEY2])
        {
            cur_idx1++;
        }
        else
        {
            cur_idx2++;
        }

        mram_read((__mram_ptr void *)(first_addr + cur_idx1 * col_num1 * sizeof(T)), first_row, col_num1 * sizeof(T));
        mram_read((__mram_ptr void *)(second_addr + cur_idx2 * col_num2 * sizeof(T)), second_row, col_num2 * sizeof(T));
    }
    
    joined_rows[tasklet_id] = cur_row_idx;

    barrier_wait(&my_barrier);

    int local_offset = 0;
    for (int t = 0; t < tasklet_id; t++) local_offset += joined_rows[t];

    res_addr += local_offset * total_col * sizeof(T);
    int write_idx = 0;
    while (write_idx < cur_row_idx) {
        mram_read((__mram_ptr void *)(first_addr + selected_1[write_idx] * col_num1 * sizeof(T)), first_row, col_num1 * sizeof(T));
        mram_read((__mram_ptr void *)(second_addr + selected_2[write_idx] * col_num2 * sizeof(T)), second_row, col_num2 * sizeof(T));

        int cur_col = 0;
        for (int c = 0; c < col_num1; c++)
        {
            merge_row[cur_col] = first_row[c];
            cur_col++;
        }
        for (int c = 0; c < col_num2; c++)
        {
            if (c == JOIN_KEY2)
                continue;
            merge_row[cur_col] = second_row[c];
            cur_col++;
        }

        printf("\nMERGE! %d %d %d %d %d %d %d\n\n", merge_row[0], merge_row[1], merge_row[2], merge_row[3], merge_row[4], merge_row[5], merge_row[6]);
        mram_write(merge_row, (__mram_ptr void *)(res_addr + write_idx * total_col * sizeof(T)), total_col * sizeof(T));
        mram_read((__mram_ptr void *)(res_addr + write_idx * total_col * sizeof(T)), merge_row, total_col * sizeof(T));
        printf("CHECK! %d %d %d %d %d %d %d\n\n", merge_row[0], merge_row[1], merge_row[2], merge_row[3], merge_row[4], merge_row[5], merge_row[6]);

        write_idx++;
    }

    barrier_wait(&my_barrier);
    if(tasklet_id == NR_TASKLETS - 1) {
        for(int t = 0; t < NR_TASKLETS; t++) joined_row += joined_rows[t];

        for(int i = 0; i < joined_row; i++) {
            mram_read((__mram_ptr void *)((uint32_t)(DPU_MRAM_HEAP_POINTER + (row_num1 * col_num1 + row_num2 * col_num2) * sizeof(T) + i * total_col * sizeof(T))), merge_row, total_col * sizeof(T));
            printf("\n!!! %d %d %d %d %d %d %d", merge_row[0], merge_row[1], merge_row[2], merge_row[3], merge_row[4], merge_row[5], merge_row[6]);
        }

        printf("\n");
    }

    mem_reset();

    return 0;
}