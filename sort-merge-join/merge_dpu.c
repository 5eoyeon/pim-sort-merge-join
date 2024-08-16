// dpu-upmem-dpurte-clang -DNR_TASKLETS=1 -o merge_dpu merge_dpu.c

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

__host int col_num;
__host int row_num;
__host int merge_array[MAX_ROW * MAX_COL];
__mram_noinit int result_array;

int main()
{
    for (int r = 0; r < row_num; r++)
    {
        for (int c = 0; c < col_num; c++)
        {
            printf("%d ", *(merge_array + r * col_num + c));
        }
        printf("\n");
    }

    return 0;
}