// dpu-upmem-dpurte-clang -DNR_TASKLETS=1 -o merge_dpu merge_dpu.c
// use 24 tasklets (max)

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

__host dpu_block_t info;
uint32_t addrs[NR_TASKLETS];
int rows[NR_TASKLETS];

int main()
{
    int col_num = info.col_num;
    int row_num = info.row_num;

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
    addrs[tasklet_id] = mram_base_addr;
    rows[tasklet_id] = row_per_tasklet;

    /* do quick sort */

    /* do merge sort */

    // 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23
    // %2: 0 2 4 6 8 10 12 14 16 18 20 22 // 0-1 2-3 ...
    // %4: 0 4 8 12 16 20 // 01-23 45-67 ...
    // %8: 0 8 16 // 0123-4567 891011-12131415 16171819-20212223
    // %16: 0 16 // 01234567-89101112131415 1614181920212223
    // %32: 0 // 0123456789101112131415-1617181920212223

    barrier_wait(&my_barrier);

    // int running = NR_TASKLETS;
    int running = 24;
    int step = 2;

    while (running > 1)
    {
        running /= 2;

        if (tasklet_id % step == 0)
        {
            int *first_row = (int *)mem_alloc(col_num * sizeof(int));
            int *second_row = (int *)mem_alloc(col_num * sizeof(int));
            int *tmp_row = (int *)mem_alloc(col_num * sizeof(int));
            int first_cnt = 0;
            int trg = tasklet_id + step / 2;

            if (trg < NR_TASKLETS)
            {
                // todo: add loop
                uint32_t first_addr = addrs[tasklet_id];
                uint32_t second_addr = addrs[trg];

                mram_read(first_addr + first_cnt * col_num * sizeof(int), first_row, col_num * sizeof(int));
                mram_read(second_addr, second_row, col_num * sizeof(int));

                if (first_row[SELECT_COL] <= second_row[SELECT_COL])
                    first_cnt++;
                else
                {
                    // exchange
                    mram_read(second_addr, tmp_row, col_num * sizeof(int));
                    mram_write(first_row, second_addr, col_num * sizeof(int));
                    mram_write(tmp_row, first_addr + first_cnt * col_num * sizeof(int), col_num * sizeof(int));

                    // re-sort in second
                    int change_idx = 1;
                    uint32_t change_addr = second_addr + change_idx * col_num * sizeof(int);

                    int *save_row = (int *)mem_alloc(col_num * sizeof(int));
                    mram_read(second_addr, save_row, col_num * sizeof(int));
                    mram_read(change_addr, tmp_row, col_num * sizeof(int));

                    mram_write(save_row, change_addr, col_num * sizeof(int));
                    first_cnt++;
                }
            }
            rows[tasklet_id] += rows[trg];
        }
        step *= 2;
        barrier_wait(&my_barrier);
    }

    barrier_wait(&my_barrier);

    mem_reset();

    return 0;
}