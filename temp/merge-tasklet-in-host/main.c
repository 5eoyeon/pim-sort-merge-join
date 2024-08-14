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

#define NR_DPUS 2
#define NR_TASKLETS 2

#define MAX_COL 10
#define MAX_ROW 10

typedef struct {
    int rows;
    int result[MAX_ROW * MAX_COL];
} tasklet_res;

int col_num = 0;
int row_num = 0;
int *test_array = NULL;
int *sorted_array = NULL;
tasklet_res result_array[NR_DPUS][NR_TASKLETS];

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
        DPU_ASSERT(dpu_copy_from(dpu, "output", 0, result_array[dpu_id], sizeof(result_array[dpu_id])));
    }
    stop(&timer, 0);

    // Print result
    for (int d = 0; d < NR_DPUS; d++) {
        printf("DPU %d results:\n", d);
        for (int t = 0; t < NR_TASKLETS; t++) {
            printf("Tasklet %d result:\n", t);
            printf("Rows: %u\n", result_array[d][t].rows);
            for (int i = 0; i < result_array[d][t].rows; i++) {
                for (int j = 0; j < col_num; j++) printf("%u ", result_array[d][t].result[i*col_num + j]);
                printf("\n");
            }
            printf("\n");
        }
    }

    // conquer
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
    print(&timer, 0, 1);
    printf("\n");

    // Merge results
    // Same in task.c

    DPU_ASSERT(dpu_free(set));
    free(test_array);

    return 0;
}