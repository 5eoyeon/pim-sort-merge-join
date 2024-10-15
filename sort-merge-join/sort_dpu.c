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
    if (bl.table_num == 0)
    {
        join_key = JOIN_KEY1;
    }
    else
    {
        join_key = JOIN_KEY2;
    }

    /* do quick sort */

    quick_sort(addr[tasklet_id], rows[tasklet_id], col_num, join_key);
    barrier_wait(&my_barrier);

    /* do merge sort */

    int running = NR_TASKLETS;
    int step = 2;

    int one_row_size = col_num * sizeof(T);
    int cache_size = CACHE_SIZE / one_row_size * one_row_size;
    if (cache_size == 0)
        cache_size = one_row_size;

    T *first_row = (T *)mem_alloc(col_num * sizeof(T));
    T *second_row = (T *)mem_alloc(col_num * sizeof(T));
    T *tmp_row = (T *)mem_alloc(col_num * sizeof(T));
    T *save_row = (T *)mem_alloc(col_num * sizeof(T));
    T *cache = (T *)mem_alloc(cache_size);

    while (running > 1)
    {
        if (tasklet_id == 0)
            running = (running + 1) / 2;
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
                    mram_read((__mram_ptr void *)(first_addr + first_cnt * col_num * sizeof(T)), first_row, col_num * sizeof(T));
                    mram_read((__mram_ptr void *)(second_addr), second_row, col_num * sizeof(T));

                    if (first_row[join_key] > second_row[join_key])
                    {
                        // exchange
                        mram_write(first_row, (__mram_ptr void *)second_addr, col_num * sizeof(T));
                        mram_write(second_row, (__mram_ptr void *)(first_addr + first_cnt * col_num * sizeof(T)), col_num * sizeof(T));

                        // re-sort in second
                        int input_size = rows[trg] * col_num * sizeof(T);
                        int temp_cache_size = cache_size;
                        int change_idx = 1;

                        mram_read((__mram_ptr void *)(second_addr), save_row, col_num * sizeof(T));
                        mram_read((__mram_ptr void *)(second_addr + change_idx * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));
                        mram_read((__mram_ptr void *)(second_addr + change_idx * col_num * sizeof(T)), cache, temp_cache_size);

                        int next_val = cache[(temp_cache_size / one_row_size - 1) * col_num + join_key];
                        while (true)
                        {
                            if (input_size == 0)
                            {
                                mram_write(cache, (__mram_ptr void *)(second_addr + (change_idx - 1) * one_row_size), temp_cache_size);
                                mram_write(save_row, (__mram_ptr void *)(second_addr + (rows[trg] - 1) * col_num * sizeof(T)), col_num * sizeof(T));
                                break;
                            }

                            if (next_val > save_row[join_key])
                            {
                                int cache_idx = 0;
                                next_val = cache[cache_idx * col_num + join_key];
                                while (next_val < save_row[join_key])
                                {
                                    cache_idx++;
                                    next_val = cache[cache_idx * col_num + join_key];

                                    if (cache_idx == temp_cache_size / one_row_size)
                                        break;
                                }

                                mram_write(cache, (__mram_ptr void *)(second_addr + (change_idx - 1) * one_row_size), cache_idx * one_row_size);
                                mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1 + cache_idx) * one_row_size), one_row_size);
                                printf("change_idx: %d, cache_idx: %d\n", change_idx, cache_idx);
                                break;
                            }
                            else
                            {
                                mram_write(cache, (__mram_ptr void *)(second_addr + (change_idx - 1) * one_row_size), temp_cache_size);
                                change_idx += temp_cache_size / one_row_size;
                                input_size -= temp_cache_size;

                                if (input_size < temp_cache_size)
                                {
                                    temp_cache_size = input_size;
                                }

                                mram_read((__mram_ptr void *)(second_addr + change_idx * one_row_size), cache, temp_cache_size);
                                next_val = cache[(temp_cache_size / one_row_size - 1) * col_num + join_key];
                            }
                        }

                        // int next_val = tmp_row[join_key];
                        // while (next_val < save_row[join_key])
                        // {
                        //     mram_write(tmp_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(T)), col_num * sizeof(T));
                        //     change_idx++;
                        //     mram_read((__mram_ptr void *)(second_addr + change_idx * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));
                        //     next_val = tmp_row[join_key];

                        //     if (change_idx == rows[trg])
                        //         break;
                        // }

                        // mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(T)), col_num * sizeof(T));
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