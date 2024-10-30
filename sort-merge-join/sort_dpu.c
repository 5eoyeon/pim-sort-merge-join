#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

#define STACK_SIZE 250

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host dpu_block_t bl;
uint32_t addr[NR_TASKLETS];
int rows[NR_TASKLETS];

// Quick sort
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

    mem_reset();
}

// Bubble sort
void bubble_sort(uint32_t addr, int row_num, int col_num, int key)
{
    int one_row_size = col_num * sizeof(T);
    T *temp_i_arr = (T *)mem_alloc(one_row_size);
    T *temp_j_arr = (T *)mem_alloc(one_row_size);

    for (int i = 0; i < row_num; i++)
    {
        bool swapped = false;
        for (int j = 0; j < row_num - i - 1; j++)
        {
            mram_read((__mram_ptr void const *)(addr + j * one_row_size), temp_i_arr, one_row_size);
            mram_read((__mram_ptr void const *)(addr + (j + 1) * one_row_size), temp_j_arr, one_row_size);

            if (temp_i_arr[key] > temp_j_arr[key])
            {
                mram_write(temp_i_arr, (__mram_ptr void *)(addr + (j + 1) * one_row_size), one_row_size);
                mram_write(temp_j_arr, (__mram_ptr void *)(addr + j * one_row_size), one_row_size);
                swapped = true;
            }
        }

        if (!swapped)
        {
            break;
        }
    }

    mem_reset();
}

// Selection sort
void selection_sort(uint32_t addr, int row_num, int col_num, int key)
{
    int one_row_size = col_num * sizeof(T);
    T *temp_i_arr = (T *)mem_alloc(one_row_size);
    T *temp_j_arr = (T *)mem_alloc(one_row_size);

    for (int i = 0; i < row_num - 1; i++)
    {
        mram_read((__mram_ptr void const *)(addr + i * one_row_size), temp_i_arr, one_row_size);
        int min_idx = i;

        for (int j = i + 1; j < row_num; j++)
        {
            mram_read((__mram_ptr void const *)(addr + j * one_row_size), temp_j_arr, one_row_size);

            if (temp_j_arr[key] < temp_i_arr[key])
            {
                min_idx = j;
                mram_read((__mram_ptr void const *)(addr + min_idx * one_row_size), temp_i_arr, one_row_size);
            }
        }

        if (i != min_idx)
        {
            mram_read((__mram_ptr void const *)(addr + i * one_row_size), temp_i_arr, one_row_size);
            mram_read((__mram_ptr void const *)(addr + min_idx * one_row_size), temp_j_arr, one_row_size);

            mram_write(temp_i_arr, (__mram_ptr void *)(addr + min_idx * one_row_size), one_row_size);
            mram_write(temp_j_arr, (__mram_ptr void *)(addr + i * one_row_size), one_row_size);
        }
    }

    mem_reset();
}

// Insertion sort
void insertion_sort(uint32_t addr, int row_num, int col_num, int key)
{
    int one_row_size = col_num * sizeof(T);
    T *temp_i_arr = (T *)mem_alloc(one_row_size);
    T *temp_j_arr = (T *)mem_alloc(one_row_size);

    for (int i = 1; i < row_num; i++)
    {
        mram_read((__mram_ptr void const *)(addr + i * one_row_size), temp_i_arr, one_row_size);
        int j = i - 1;

        while (j >= 0)
        {
            mram_read((__mram_ptr void const *)(addr + j * one_row_size), temp_j_arr, one_row_size);

            if (temp_j_arr[key] > temp_i_arr[key])
            {
                mram_write(temp_j_arr, (__mram_ptr void *)(addr + (j + 1) * one_row_size), one_row_size);
                j--;
            }
            else
            {
                break;
            }
        }

        mram_write(temp_i_arr, (__mram_ptr void *)(addr + (j + 1) * one_row_size), one_row_size);
    }

    mem_reset();
}

int main()
{
    /* **************** */
    /*     Allocate     */
    /* **************** */

    // Initialize variables
    unsigned int tasklet_id = me();
    int using_tasklets = NR_TASKLETS;
    int col_num = bl.col_num;
    int row_num = bl.row_num;
    int one_row_size = col_num * sizeof(T);
    unsigned int join_key = bl.table_num == 0 ? JOIN_KEY1 : JOIN_KEY2;

    // Calculate the number of rows to process per tasklet
    int row_per_tasklet = row_num / NR_TASKLETS;
    if (row_per_tasklet == 0)
    {
        using_tasklets = 1;
        row_per_tasklet = tasklet_id == 0 ? row_num : 0;
    }

    /// Calculate the chunk size and offset from the base address
    int chunk_size = row_per_tasklet * col_num;
    int start = tasklet_id * chunk_size;
    if (tasklet_id == using_tasklets - 1)
    {
        row_per_tasklet = row_num - (using_tasklets - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    // Initialize the start address
    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER + start * sizeof(T);

    // Save the address and the number of rows
    addr[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    /* ************ */
    /*     Sort     */
    /* ************ */

    // Sort the data
    insertion_sort(addr[tasklet_id], rows[tasklet_id], col_num, join_key);

    // Barrier
    barrier_wait(&my_barrier);

    /* *************************** */
    /*     Merge with tasklets     */
    /* *************************** */

    // Initialize variables
    int running = using_tasklets;
    int step = 2;

    // Initialize local caches
    T *first_row = (T *)mem_alloc(one_row_size);
    T *second_row = (T *)mem_alloc(one_row_size);
    T *tmp_row = (T *)mem_alloc(one_row_size);
    T *save_row = (T *)mem_alloc(one_row_size);

    while (running > 1)
    {
        // If the tasklet_id is 0, reduce the running count by half
        if (tasklet_id == 0)
            running = (running + 1) / 2;

        // If the tasklet_id is a multiple of the step value
        if (tasklet_id % step == 0)
        {
            // Calculate the target tasklet
            int first_cnt = 0;
            int trg = tasklet_id + step / 2;

            // If the target tasklet is within the range
            if (trg < using_tasklets)
            {
                // Initialize the addresses
                uint32_t first_addr = addr[tasklet_id];
                uint32_t second_addr = addr[trg];

                while (first_cnt < rows[tasklet_id])
                {
                    // Load the rows
                    mram_read((__mram_ptr void *)(first_addr + first_cnt * one_row_size), first_row, one_row_size);
                    mram_read((__mram_ptr void *)(second_addr), second_row, one_row_size);

                    // If the first_row[join_key] is greater than the second_row[join_key]
                    if (first_row[join_key] > second_row[join_key])
                    {
                        // Exchange
                        mram_write(first_row, (__mram_ptr void *)second_addr, one_row_size);
                        mram_write(second_row, (__mram_ptr void *)(first_addr + first_cnt * one_row_size), one_row_size);

                        // Re-sort in second
                        if (rows[trg] > 1)
                        {
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

                                if (change_idx == rows[trg])
                                    break;
                            }

                            mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * one_row_size), one_row_size);
                        }
                    }

                    // Increment the first count
                    first_cnt++;
                }

                // Update the number of rows
                rows[tasklet_id] += rows[trg];
            }

            // Multiply the step value by 2
            step *= 2;
        }

        // Barrier
        barrier_wait(&my_barrier);
    }

#ifdef DEBUG
    mutex_lock(my_mutex);
    printf("Sort Tasklet %d: %d\n", tasklet_id, rows[tasklet_id]);
    mutex_unlock(my_mutex);
#endif

    // Reset the heap
    mem_reset();

    return 0;
}