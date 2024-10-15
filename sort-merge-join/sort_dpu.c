#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

#define STACK_SIZE 50

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host dpu_block_t bl;

uint32_t addr[NR_TASKLETS];
T rows[NR_TASKLETS];

void quick_sort(T *input, int size, int col_num, int key)
{
    int row_num = size / (col_num * sizeof(T));

    T *stack = (T *)mem_alloc(STACK_SIZE * sizeof(T));
    int top = -1;

    stack[++top] = 0;
    stack[++top] = row_num - 1;

    while (top >= 0)
    {
        int pRight = stack[top--];
        int pLeft = stack[top--];

        T pivot = input[(pLeft + pRight) / 2 * col_num + key];

        int i = pLeft;
        int j = pRight;

        while (i <= j)
        {
            while (input[i * col_num + key] < pivot && i <= j)
                i++;
            while (input[j * col_num + key] > pivot && i <= j)
                j--;

            if (i <= j)
            {
                T *temp = (T *)mem_alloc(col_num * sizeof(T));
                memcpy(temp, input + i * col_num, col_num * sizeof(T));
                memcpy(input + i * col_num, input + j * col_num, col_num * sizeof(T));
                memcpy(input + j * col_num, temp, col_num * sizeof(T));

                i++;
                j--;
            }
        }

        if (pLeft < j)
        {
            stack[++top] = pLeft;
            stack[++top] = j;
        }
        if (i < pRight)
        {
            stack[++top] = i;
            stack[++top] = pRight;
        }
    }
}

void merge(uint32_t first_addr, uint32_t second_addr, int first_row_num, int second_row_num, int col_num, int key)
{
    T *first_row = (T *)mem_alloc(col_num * sizeof(T));
    T *second_row = (T *)mem_alloc(col_num * sizeof(T));
    T *tmp_row = (T *)mem_alloc(col_num * sizeof(T));
    T *save_row = (T *)mem_alloc(col_num * sizeof(T));
    int first_cnt = 0;

    while (first_cnt < first_row_num)
    {
        mram_read((__mram_ptr void const *)(first_addr + first_cnt * col_num * sizeof(T)), first_row, col_num * sizeof(T));
        mram_read((__mram_ptr void const *)(second_addr), second_row, col_num * sizeof(T));

        if (first_row[key] > second_row[key])
        {
            // exchange
            mram_write(first_row, (__mram_ptr void *)second_addr, col_num * sizeof(T));
            mram_write(second_row, (__mram_ptr void *)(first_addr + first_cnt * col_num * sizeof(T)), col_num * sizeof(T));

            // re-sort in second
            int change_idx = 1;

            mram_read((__mram_ptr void const *)(second_addr), save_row, col_num * sizeof(T));
            mram_read((__mram_ptr void const *)(second_addr + change_idx * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));

            int next_val = tmp_row[key];
            while (next_val < save_row[key])
            {
                mram_write(tmp_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(T)), col_num * sizeof(T));
                change_idx++;

                mram_read((__mram_ptr void const *)(second_addr + change_idx * col_num * sizeof(T)), tmp_row, col_num * sizeof(T));
                next_val = tmp_row[key];

                if (change_idx == second_row_num)
                    break;
            }

            mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(T)), col_num * sizeof(T));
        }

        first_cnt++;
    }
}

int main()
{
    // -------------------- Allocate --------------------

    unsigned int tasklet_id = me();
    int col_num = bl.col_num;
    int row_num = bl.row_num;
    unsigned int join_key = bl.table_num == 0 ? JOIN_KEY1 : JOIN_KEY2;

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

    int one_row_size = col_num * sizeof(T);
    int cache_size = CACHE_SIZE / one_row_size * one_row_size;
    if (cache_size == 0)
        cache_size = one_row_size;
    int input_tasklet_size = row_per_tasklet * col_num * sizeof(T);

    if (tasklet_id == 0)
    {
        mem_reset();
    }

    barrier_wait(&my_barrier);

    // -------------------- Sort --------------------

    // uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER;
    T *cache = (T *)mem_alloc(cache_size);
    int cnt = (input_tasklet_size + cache_size - 1) / cache_size;
    uint32_t *tasklet_addr = (uint32_t *)mem_alloc(cnt * sizeof(uint32_t));
    uint32_t *tasklet_rows = (uint32_t *)mem_alloc(cnt * sizeof(uint32_t));

    for (int byte_index = 0; byte_index < input_tasklet_size; byte_index += cache_size)
    {
        bool is_last = input_tasklet_size - byte_index <= cache_size;
        if (is_last)
        {
            cache_size = input_tasklet_size - byte_index;
        }

        tasklet_addr[byte_index / cache_size] = mram_base_addr + byte_index;
        tasklet_rows[byte_index / cache_size] = cache_size / one_row_size;
        mram_read((__mram_ptr void const *)(mram_base_addr + byte_index), cache, cache_size);
        quick_sort(cache, cache_size, col_num, join_key);

        mram_write(cache, (__mram_ptr void *)(mram_base_addr + byte_index), cache_size);
    }

    barrier_wait(&my_barrier);

    // -------------------- Merge in tasklet --------------------

    int step = 2;

    while (cnt > 1)
    {
        for (int i = 0; i < cnt; i += step)
        {
            merge(tasklet_addr[i], tasklet_addr[i + step / 2], tasklet_rows[i], tasklet_rows[i + step / 2], col_num, join_key);
        }
        cnt = (cnt + 1) / 2;
        step *= 2;
    }

    if (tasklet_id == 0)
    {
        bl.row_num = tasklet_rows[0];
    }

    barrier_wait(&my_barrier);

    // -------------------- Merge in dpu --------------------

    int running = NR_TASKLETS;
    step = 2;

    T *first_row = (T *)mem_alloc(col_num * sizeof(T));
    T *second_row = (T *)mem_alloc(col_num * sizeof(T));
    T *tmp_row = (T *)mem_alloc(col_num * sizeof(T));
    T *save_row = (T *)mem_alloc(col_num * sizeof(T));

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

                            if (change_idx == rows[trg])
                                break;
                        }

                        mram_write(save_row, (__mram_ptr void *)(second_addr + (change_idx - 1) * col_num * sizeof(T)), col_num * sizeof(T));
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
    printf("Sort Tasklet %d: %lld\n", tasklet_id, rows[tasklet_id]);
    mutex_unlock(my_mutex);
#endif
    mem_reset();

    return 0;
}
