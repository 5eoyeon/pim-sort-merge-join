#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <handshake.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

#define CACHE_SIZE 256

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host dpu_block_t bl;

uint32_t message[NR_TASKLETS];
uint32_t message_partial_count;

int cycle_cnt[NR_TASKLETS];

// Selects rows from the input based on a specified condition and copies them to the output
unsigned int select(int tasklet_id, T *input, T *output, int size, int col_num, int select_col, T select_val)
{
    unsigned int cnt = 0;
    int row_num = size / (col_num * sizeof(T));

    for (int i = 0; i < row_num; i++)
    {
        if (input[i * col_num + select_col] > select_val)
        {
            memcpy(output + cnt * col_num, input + i * col_num, col_num * sizeof(T));
            cnt++;
        }
    }

    return cnt;
}

// Synchronizes tasklets using a handshaking mechanism
unsigned int handshake_sync(unsigned int l_count, unsigned int tasklet_id, bool is_last)
{
    unsigned int p_count;

    if (tasklet_id != 0)
    {
        handshake_wait_for(tasklet_id - 1);
        p_count = message[tasklet_id];
    }
    else
        p_count = 0;

    if (tasklet_id < NR_TASKLETS - 1 && !is_last)
    {
        message[tasklet_id + 1] = p_count + l_count;
        handshake_notify();
    }

    return p_count;
}

int main()
{
    /* **************** */
    /*     Allocate     */
    /* **************** */

    // Initialize variables
    unsigned int tasklet_id = me();
    int col_num = bl.col_num;
    int row_num = bl.row_num;
    unsigned int select_col = bl.table_num == 0 ? SELECT_COL1 : SELECT_COL2;
    unsigned int select_val = bl.table_num == 0 ? SELECT_VAL1 : SELECT_VAL2;

    // Calculate sizes
    int one_row_size = col_num * sizeof(T);
    int cache_size = CACHE_SIZE / one_row_size * one_row_size;
    int input_size = row_num * col_num * sizeof(T);

    // Calculate the number of rows to process per tasklet
    int row_per_tasklet = row_num / NR_TASKLETS == 0 ? 1 : row_num / NR_TASKLETS;
    if (cache_size > row_per_tasklet * one_row_size)
        cache_size = row_per_tasklet * one_row_size;

    // Reset the heap ocne
    if (tasklet_id == 0)
    {
        mem_reset();
    }

    // Barrier
    barrier_wait(&my_barrier);

    /* ************** */
    /*     Select     */
    /* ************** */

    // Initialize the start address and local caches
    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER;
    T *cache_A = (T *)mem_alloc(cache_size);
    T *cache_B = (T *)mem_alloc(cache_size);

    // Initialize shared variables
    if (tasklet_id == NR_TASKLETS - 1)
        message_partial_count = 0;

    // Calculate the number of cycles
    int cnt = (input_size - tasklet_id * cache_size + cache_size * NR_TASKLETS - 1) / (cache_size * NR_TASKLETS);
    cycle_cnt[tasklet_id] = cnt;

    // Find the max cycle count among all tasklets
    int cycle = cycle_cnt[0];
    for (int i = 1; i < NR_TASKLETS; i++)
    {
        if (cycle_cnt[i] > cycle)
        {
            cycle = cycle_cnt[i];
        }
    }

    // Barrier
    barrier_wait(&my_barrier);

    for (int byte_index = tasklet_id * cache_size, i = 0; i < cycle; byte_index += cache_size * NR_TASKLETS, i++)
    {
        // If the tasklet has run fewer cycles than cycle_cnt, wait for the other tasklets
        if (i >= cycle_cnt[tasklet_id])
        {
            barrier_wait(&my_barrier);
            barrier_wait(&my_barrier);
            continue;
        }

        // If the tasklet is processing the last cycle, adjust the cache size
        bool is_last = input_size - byte_index <= cache_size;
        if (is_last)
        {
            cache_size = input_size - byte_index;
        }

        // Load data from MRAM to cache
        mram_read((__mram_ptr void const *)(mram_base_addr + byte_index), cache_A, cache_size);

        // Select rows
        uint32_t l_count = select(tasklet_id, cache_A, cache_B, cache_size, col_num, select_col, select_val);

        // Synchronize tasklets
        uint32_t p_count = handshake_sync(l_count, tasklet_id, is_last);

        // Barrier
        barrier_wait(&my_barrier);

        if (l_count > 0)
        {
            mram_write(cache_B, (__mram_ptr void *)(mram_base_addr + (message_partial_count + p_count) * one_row_size), l_count * one_row_size);
        }

        // Update total row count
        if (tasklet_id == NR_TASKLETS - 1)
        {
            bl.row_num = message_partial_count + p_count + l_count;
            message_partial_count = bl.row_num;
        }
        else if (is_last)
        {
            if (i == 0)
            {
                bl.row_num = 0;
            }
            bl.row_num += p_count + l_count;
        }

        // Barrier
        barrier_wait(&my_barrier);

#ifdef DEBUG
        mutex_lock(my_mutex);
        if (is_last)
        {
            printf("Table %d : select %d rows\n", bl.table_num, bl.row_num);
        }
        mutex_unlock(my_mutex);
#endif
    }

    // Barrier
    barrier_wait(&my_barrier);

    // Reset the heap
    mem_reset();

    return 0;
}
