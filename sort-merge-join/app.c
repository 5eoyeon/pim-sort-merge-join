// gcc --std=c99 app.c -o app `dpu-pkg-config --cflags --libs dpu`

#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>
#include <stdlib.h>
#include "timer.h"
#include "common.h"

#ifndef DPU_BINARY
#define DPU_BINARY "./task"
#endif

#ifndef DPU_BINARY_1
#define DPU_BINARY_1 "./merge_dpu"
#endif

int col_num = 0;
int row_num = 0;
int total_row_num = 0;
int *test_array = NULL;
dpu_result_t dpu_result[NR_DPUS];

void set_csv_size(const char *filename)
{
    FILE *file = fopen(filename, "r");
    if (!file)
    {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }

    char line[1024];
    int is_first_line = 1;

    while (fgets(line, sizeof(line), file))
    {
        if (is_first_line)
        {
            is_first_line = 0;
            char *token = strtok(line, ",");
            while (token)
            {
                col_num++;
                token = strtok(NULL, ",");
            }
        }
        row_num++;
    }

    row_num--;
    fclose(file);
}

void load_csv(const char *filename)
{
    test_array = (int *)malloc(col_num * row_num * sizeof(int));

    FILE *file = fopen(filename, "r");
    if (!file)
    {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }

    char line[1024];
    int row = 0;

    // Skip header line
    fgets(line, sizeof(line), file);

    while (fgets(line, sizeof(line), file))
    {
        char *token = strtok(line, ",");
        int col = 0;
        while (token)
        {
            test_array[row * col_num + col] = atoi(token);
            token = strtok(NULL, ",");
            col++;
        }
        row++;
    }

    fclose(file);
}

int main(void)
{
    /* ************************ */
    /* select & sort per tasklet */
    /* ************************ */

    // Allocate DPUs
    struct dpu_set_t set, dpu;
    uint32_t dpu_id;
    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));

    // Set col_num, row_num
    set_csv_size(FILE_NAME);
    int row_size = row_num / NR_DPUS;

    // Set test_array
    load_csv(FILE_NAME);

    // Set timer
    Timer timer;

    // Set input arguments
    dpu_block_t input_args[NR_DPUS];
    for (int i = 0; i < NR_DPUS - 1; i++)
    {
        input_args[i].col_num = col_num;
        input_args[i].row_num = row_size;
    }
    input_args[NR_DPUS - 1].col_num = col_num;
    input_args[NR_DPUS - 1].row_num = row_num - (NR_DPUS - 1) * row_size;

    // Transfer input arguments and test_array to DPUs
    start(&timer, 0, 0);
    DPU_FOREACH(set, dpu, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &input_args[dpu_id]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));

    DPU_FOREACH(set, dpu, dpu_id)
    {
        int offset = dpu_id * row_size * col_num;
        int transfer_size = input_args[dpu_id].row_num * input_args[dpu_id].col_num * sizeof(int);
        DPU_ASSERT(dpu_prepare_xfer(dpu, test_array + offset));
        DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    }

    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    // Retrieve dpu_result from DPUs
    // DPU_FOREACH(set, dpu, dpu_id)
    // {
    //     DPU_ASSERT(dpu_prepare_xfer(dpu, &input_args[dpu_id]));
    //     DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));
    //     dpu_result[dpu_id].dpu_id = dpu_id;
    //     dpu_result[dpu_id].col_num = input_args[dpu_id].col_num;
    //     dpu_result[dpu_id].row_num = input_args[dpu_id].row_num;
    //     total_row_num += dpu_result[dpu_id].row_num;

    //     int transfer_size = dpu_result[dpu_id].row_num * dpu_result[dpu_id].col_num * sizeof(int);
    //     dpu_result[dpu_id].arr = (int *)malloc(transfer_size);
    //     DPU_ASSERT(dpu_prepare_xfer(dpu, dpu_result[dpu_id].arr));
    //     DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    // }
    stop(&timer, 0);

#ifdef DEBUG
    // Print DPU logs
    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

    // Print result
    printf("===============\n");
    // for (int d = 0; d < NR_DPUS; d++)
    // {
    //     printf("DPU %d results:\n", dpu_result[d].dpu_id);
    //     printf("Rows: %u\n", dpu_result[d].row_num);
    //     // for (int i = 0; i < dpu_result[d].row_num; i++)
    //     // {
    //     //     for (int j = 0; j < col_num; j++)
    //     //     {
    //     //         printf("%d ", dpu_result[d].arr[i * col_num + j]);
    //     //     }
    //     //     printf("\n");
    //     // }
    //     printf("---------------\n");
    // }
    // printf("total_row_num: %d\n", total_row_num);
    print(&timer, 0, 1);
    printf("\n");
#endif

    DPU_ASSERT(dpu_free(set));

    /* ************************ */
    /* add & sort DPU results */
    /* ************************ */

    // merge each dpu results
    // struct dpu_set_t set1, dpu1; // modify label later
    // uint32_t dpu_id1;

    // DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set1)); // change # of DPUs
    // DPU_ASSERT(dpu_load(set1, DPU_BINARY_1, NULL));

    // // vars for assign
    // int current_level = 1;
    // int assign_row = total_row_num / NR_DPUS;
    // while (assign_row <= total_row_num)
    // {
    //     assign_row = (total_row_num / NR_DPUS) * current_level;
    //     int size = col_num * (assign_row) * sizeof(int);
    //     int cur_dpu_idx = 0;
    //     int cur_row_idx = 0;
    //     int cur_row_num = 0;

    //     // assign dpu depending on NR_DPUS & dpu_result size
    //     DPU_FOREACH(set1, dpu1, dpu_id1)
    //     {
    //         int *merge_array;
    //         if (dpu_id1 == NR_DPUS - 1)
    //         {
    //             assign_row = total_row_num - assign_row * (NR_DPUS - 1);
    //             size = col_num * assign_row * sizeof(int);
    //         }
    //         merge_array = (int *)malloc(size);

    //         int is_complete = 0;
    //         for (int d = cur_dpu_idx; d < NR_DPUS; d++)
    //         {
    //             for (int r = cur_row_idx; r < dpu_result[d].row_num; r++)
    //             {
    //                 for (int c = 0; c < col_num; c++)
    //                 {
    //                     merge_array[cur_row_num * col_num + c] = dpu_result[d].arr[r * col_num + c];
    //                 }
    //                 cur_row_num++;
    //                 cur_row_idx++;
    //                 if (cur_row_idx == dpu_result[d].row_num)
    //                     cur_row_idx = 0;

    //                 if (cur_row_num == assign_row)
    //                 {
    //                     is_complete = 1;
    //                     cur_row_num = 0; // for next assign
    //                     break;
    //                 }
    //             }

    //             if (is_complete)
    //                 break;

    //             cur_dpu_idx++;
    //         }

    //         DPU_ASSERT(dpu_prepare_xfer(dpu1, &col_num));
    //         DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_TO_DPU, "col_num", 0, sizeof(int), DPU_XFER_DEFAULT));
    //         DPU_ASSERT(dpu_prepare_xfer(dpu1, &assign_row));
    //         DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_TO_DPU, "row_num", 0, sizeof(int), DPU_XFER_DEFAULT));

    //         DPU_ASSERT(dpu_prepare_xfer(dpu1, merge_array));
    //         DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_TO_DPU, "merge_array", 0, size, DPU_XFER_DEFAULT));
    //     }

    //     DPU_ASSERT(dpu_launch(set1, DPU_SYNCHRONOUS));

    //     DPU_FOREACH(set1, dpu1)
    //     {
    //         DPU_ASSERT(dpu_log_read(dpu1, stdout));
    //     }

    //     // Retrieve dpu_result from DPUs
    //     // overwritten `dpu_result` and reuse in loop
    //     DPU_FOREACH(set1, dpu1, dpu_id1)
    //     {
    //         DPU_ASSERT(dpu_prepare_xfer(dpu1, dpu_result + dpu_id1));
    //         DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_FROM_DPU, "dpu_result", 0, sizeof(int), DPU_XFER_DEFAULT));
    //         dpu_result[dpu_id1].dpu_id = dpu_id1;
    //         total_row_num += dpu_result[dpu_id1].row_num;
    //     }
    // }

    return 0;
}