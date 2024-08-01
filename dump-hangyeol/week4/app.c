// gcc --std=c99 app.c -o app `dpu-pkg-config --cflags --libs dpu`

#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef DPU_BINARY
#define DPU_BINARY "./task"
#endif

#define NR_DPUS 2
#define NR_TASKLETS 2

int col_num = 0;
int row_num = 0;
int *test_array = NULL;

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

    // Transfer test_array to DPUs
    DPU_FOREACH(set, dpu, dpu_id)
    {
        int offset = dpu_id * row_size * col_num;
        int rows_to_transfer = (dpu_id == NR_DPUS - 1) ? (row_num - dpu_id * row_size) : row_size; // 마지막 DPU는 남은 row 전부 전송

        DPU_ASSERT(dpu_prepare_xfer(dpu, &col_num));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "col_num", 0, sizeof(int), DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_prepare_xfer(dpu, &rows_to_transfer));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "row_num", 0, sizeof(int), DPU_XFER_DEFAULT));

        DPU_ASSERT(dpu_prepare_xfer(dpu, test_array + offset));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_TO_DPU, "test_array", 0, rows_to_transfer * col_num * sizeof(int), DPU_XFER_DEFAULT));
    }

    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    // Retrieve test_array from DPUs
    DPU_FOREACH(set, dpu, dpu_id)
    {
        int offset = dpu_id * row_size * col_num;
        int rows_to_transfer = (dpu_id == NR_DPUS - 1) ? (row_num - dpu_id * row_size) : row_size;

        DPU_ASSERT(dpu_prepare_xfer(dpu, test_array + offset));
        DPU_ASSERT(dpu_push_xfer(dpu, DPU_XFER_FROM_DPU, "test_array", 0, rows_to_transfer * col_num * sizeof(int), DPU_XFER_DEFAULT));
    }

    // Print DPU logs
    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

    // Print result
    for (int i = 0; i < row_num; i++)
    {
        for (int j = 0; j < col_num; j++)
        {
            printf("%d ", test_array[i * col_num + j]);
        }
        printf("\n");
    }

    DPU_ASSERT(dpu_free(set));
    free(test_array);

    return 0;
}