// gcc --std=c99 main.c -o main `dpu-pkg-config --cflags --libs dpu`

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

int col_num = 0;
int row_num = 0;
int *test_array = NULL;
dpu_result result_array[NR_DPUS];

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
    // Allocate DPUs
    struct dpu_set_t set, dpu;
    uint32_t dpu_id;
    DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));

    // Set col_num, row_num
    set_csv_size("test_data.csv");
    int row_size = row_num / NR_DPUS;

    // Set test_array
    load_csv("test_data.csv");

    // Set timer
    Timer timer;

    // Transfer test_array to DPUs
    start(&timer, 0, 0);
    DPU_FOREACH(set, dpu, dpu_id)
    {
        int offset = dpu_id * row_size * col_num;
        int rows_to_transfer = (dpu_id == NR_DPUS - 1) ? (row_num - dpu_id * row_size) : row_size;

        DPU_ASSERT(dpu_prepare_xfer(dpu, &col_num));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "col_num", 0, sizeof(int), DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_prepare_xfer(dpu, &rows_to_transfer));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "row_num", 0, sizeof(int), DPU_XFER_DEFAULT));

        DPU_ASSERT(dpu_prepare_xfer(dpu, test_array + offset));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "test_array", 0, rows_to_transfer * col_num * sizeof(int), DPU_XFER_DEFAULT));
    }

    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    // Retrieve result_array from DPUs
    DPU_FOREACH(set, dpu, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, result_array + dpu_id));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "result_array", 0, sizeof(dpu_result), DPU_XFER_DEFAULT));
        result_array[dpu_id].dpu_id = dpu_id;
    }
    stop(&timer, 0);

#ifdef DEBUG
    // Print DPU logs
    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

    // Print result
    printf("===============\n");
    for (int d = 0; d < NR_DPUS; d++)
    {
        printf("DPU %d results:\n", result_array[d].dpu_id);
        printf("Rows: %u\n", result_array[d].row_num);
        for (int i = 0; i < result_array[d].row_num; i++)
        {
            for (int j = 0; j < col_num; j++)
            {
                printf("%d ", result_array[d].arr[i * col_num + j]);
            }
            printf("\n");
        }
        printf("---------------\n");
    }
    print(&timer, 0, 1);
    printf("\n");
#endif

    // Merge results
    // Same in task.c

    DPU_ASSERT(dpu_free(set));
    free(test_array);

    return 0;
}