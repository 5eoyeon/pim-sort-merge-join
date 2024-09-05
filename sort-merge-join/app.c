// gcc --std=c99 app.c -o app `dpu-pkg-config --cflags --libs dpu`

#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>
#include <stdlib.h>
#include "timer.h"
#include "common.h"

#ifndef DPU_BINARY_SELECT
#define DPU_BINARY_SELECT "./select"
#endif

#ifndef DPU_BINARY_SORT_DPU
#define DPU_BINARY_SORT_DPU "./sort_dpu"
#endif

#ifndef DPU_BINARY_MERGE_DPU
#define DPU_BINARY_MERGE_DPU "./merge_dpu"
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
    /* ****************** */
    /* select per tasklet */
    /* ****************** */

    // Allocate DPUs
    struct dpu_set_t set, dpu;
    uint32_t dpu_id;
    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY_SELECT, NULL));

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
    DPU_FOREACH(set, dpu, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &input_args[dpu_id]));
        DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));
        dpu_result[dpu_id].dpu_id = dpu_id;
        dpu_result[dpu_id].col_num = input_args[dpu_id].col_num;
        dpu_result[dpu_id].row_num = input_args[dpu_id].row_num;
        total_row_num += dpu_result[dpu_id].row_num;

        int transfer_size = dpu_result[dpu_id].row_num * dpu_result[dpu_id].col_num * sizeof(int);
        dpu_result[dpu_id].arr = (int *)malloc(transfer_size);
        DPU_ASSERT(dpu_prepare_xfer(dpu, dpu_result[dpu_id].arr));
        DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    }

    int *select_array = (int *)malloc(col_num * total_row_num * sizeof(int));
    int offset = 0;
    for (int i = 0; i < NR_DPUS; i++)
    {
        int size = dpu_result[i].row_num * dpu_result[i].col_num;
        memcpy(select_array + offset, dpu_result[i].arr, size * sizeof(int));
        offset += size;
    }

    stop(&timer, 0);

#ifdef DEBUG
    // Print DPU logs
    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
#endif

    DPU_ASSERT(dpu_free(set));

    /* ************************ */
    /*     sort in each DPU     */
    /* ************************ */

    // Allocate DPUs
    struct dpu_set_t set1, dpu1;
    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set1));
    DPU_ASSERT(dpu_load(set1, DPU_BINARY_SORT_DPU, NULL));

    // Set input arguments
    row_size = total_row_num / NR_DPUS;
    for (int i = 0; i < NR_DPUS - 1; i++)
    {
        input_args[i].col_num = col_num;
        input_args[i].row_num = row_size;
        dpu_result[i].row_num = row_size;
    }
    input_args[NR_DPUS - 1].col_num = col_num;
    input_args[NR_DPUS - 1].row_num = total_row_num - (NR_DPUS - 1) * row_size;
    dpu_result[NR_DPUS - 1].row_num = total_row_num - (NR_DPUS - 1) * row_size;

    // Transfer input arguments and test_array to DPUs
    start(&timer, 1, 0);
    DPU_FOREACH(set1, dpu1, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu1, &input_args[dpu_id]));
    }
    DPU_ASSERT(dpu_push_xfer(set1, DPU_XFER_TO_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));

    DPU_FOREACH(set1, dpu1, dpu_id)
    {
        int transfer_size = input_args[dpu_id].row_num * input_args[dpu_id].col_num * sizeof(int);
        dpu_result[dpu_id].arr = (int *)malloc(transfer_size);
        DPU_ASSERT(dpu_prepare_xfer(dpu1, select_array + dpu_id * row_size * col_num));
        DPU_ASSERT(dpu_push_xfer(set1, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    }

    DPU_ASSERT(dpu_launch(set1, DPU_SYNCHRONOUS));

    // Retrieve dpu_result from DPUs
    DPU_FOREACH(set1, dpu1, dpu_id)
    {
        int transfer_size = input_args[dpu_id].row_num * input_args[dpu_id].col_num * sizeof(int);
        DPU_ASSERT(dpu_prepare_xfer(dpu1, dpu_result[dpu_id].arr));
        DPU_ASSERT(dpu_push_xfer(set1, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    }
    stop(&timer, 1);

#ifdef DEBUG
    // Print DPU logs
    DPU_FOREACH(set1, dpu1)
    {
        DPU_ASSERT(dpu_log_read(dpu1, stdout));
    }

    // Print result
    printf("===============\n");
    for (int d = 0; d < NR_DPUS; d++)
    {
        printf("DPU %d results:\n", dpu_result[d].dpu_id);
        printf("Rows: %u\n", dpu_result[d].row_num);
        for (int i = 0; i < dpu_result[d].row_num; i++)
        {
            for (int j = 0; j < col_num; j++)
            {
                printf("%d ", dpu_result[d].arr[i * col_num + j]);
            }
            printf("\n");
        }
        printf("---------------\n");
    }
    printf("total_row_num: %d\n", total_row_num);
    print(&timer, 0, 1);
    printf("\n");
#endif

    DPU_ASSERT(dpu_free(set1));
    free(test_array);
    free(select_array);

    /* ********************** */
    /* add & sort DPU results */
    /* ********************** */

    // merge each dpu results
    struct dpu_set_t set2, dpu2; // modify label later

    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set2)); // change # of DPUs
    DPU_ASSERT(dpu_load(set2, DPU_BINARY_MERGE_DPU, NULL));

    // Transfer input arguments and test_array to DPUs
    DPU_FOREACH(set2, dpu2, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu1, &input_args[dpu_id]));
    }
    DPU_ASSERT(dpu_push_xfer(set1, DPU_XFER_TO_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));

    // vars for assign
    int running = NR_DPUS;
    while (running > 1)
    {   
        running /= 2;
        // assign dpu depending on NR_DPUS & dpu_result size
        DPU_FOREACH(set2, dpu2, dpu_id)
        {

            DPU_ASSERT(dpu_prepare_xfer(dpu2, &col_num));
            DPU_ASSERT(dpu_push_xfer(dpu2, DPU_XFER_TO_DPU, "col_num", 0, sizeof(int), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu2, &assign_row));
            DPU_ASSERT(dpu_push_xfer(dpu2, DPU_XFER_TO_DPU, "row_num", 0, sizeof(int), DPU_XFER_DEFAULT));

            DPU_ASSERT(dpu_prepare_xfer(dpu2, merge_array));
            DPU_ASSERT(dpu_push_xfer(dpu2, DPU_XFER_TO_DPU, "merge_array", 0, size, DPU_XFER_DEFAULT));
        }

        DPU_ASSERT(dpu_launch(set2, DPU_SYNCHRONOUS));

        DPU_FOREACH(set2, dpu2)
        {
            DPU_ASSERT(dpu_log_read(dpu2, stdout));
        }

        // Retrieve dpu_result from DPUs
        // overwritten `dpu_result` and reuse in loop
        DPU_FOREACH(set2, dpu2, dpu_id2)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu2, dpu_result + dpu_id2));
            DPU_ASSERT(dpu_push_xfer(dpu2, DPU_XFER_FROM_DPU, "dpu_result", 0, sizeof(int), DPU_XFER_DEFAULT));
            dpu_result[dpu_id2].dpu_id = dpu_id2;
            total_row_num += dpu_result[dpu_id2].row_num;
        }
    }

    return 0;
}
