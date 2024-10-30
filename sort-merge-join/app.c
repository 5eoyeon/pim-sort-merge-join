#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>
#include <stdlib.h>
#include "timer.h"
#include "common.h"
#include "user.h"

#ifndef DPU_BINARY_SELECT
#define DPU_BINARY_SELECT "./select"
#endif

#ifndef DPU_BINARY_SORT_DPU
#define DPU_BINARY_SORT_DPU "./sort_dpu"
#endif

#ifndef DPU_BINARY_MERGE_DPU
#define DPU_BINARY_MERGE_DPU "./merge_dpu"
#endif

#ifndef DPU_BINARY_JOIN
#define DPU_BINARY_JOIN "./join"
#endif

dpu_result_t dpu_result[NR_DPUS];

void set_csv_size(const char *filename, int *col_num, int *row_num)
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
                (*col_num)++;
                token = strtok(NULL, ",");
            }
        }
        (*row_num)++;
    }

    (*row_num)--;
    fclose(file);
}

void load_csv(const char *filename, int col_num, int row_num, T **test_array)
{
    // Allocate memory for the array
    *test_array = (T *)malloc(col_num * row_num * sizeof(T));

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

    // Read each row from the file
    while (fgets(line, sizeof(line), file))
    {
        char *token = strtok(line, ",");
        int col = 0;
        while (token)
        {
            (*test_array)[row * col_num + col] = atoi(token);
            token = strtok(NULL, ",");
            col++;
        }
        row++;
    }

    fclose(file);
}

int binary_search(dpu_result_t *table, int key_col, T target)
{
    int left = 0;
    int right = table->row_num - 1;
    int idx = -1;

    while (left <= right)
    {
        int mid = (left + right) / 2;
        T *mid_row = &table->arr[mid * table->col_num];

        if (mid_row[key_col] == target)
        {
            return mid;
        }
        else if (mid_row[key_col] < target)
        {
            idx = mid;
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }

    return idx;
}

int main(int argc, char *argv[])
{
    /* ************************** */
    /*     select per tasklet     */
    /* ************************** */

    // Get file name
    const char *FILE_NAME1 = argv[1];
    const char *FILE_NAME2 = argv[2];

    // Set timer
    Timer timer;

    double cpu_dpu_time = 0.0;
    double dpu_time = 0.0;
    double dpu_cpu_time = 0.0;

    // Set variables
    int col_num1 = 0;
    int row_num1 = 0;
    int total_row_num1 = 0;
    int col_num2 = 0;
    int row_num2 = 0;
    int total_row_num2 = 0;

    T *test_array1 = NULL;
    T *test_array2 = NULL;
    int pivot_id = -1;

    // Set col_num, row_num
    set_csv_size(FILE_NAME1, &col_num1, &row_num1);
    set_csv_size(FILE_NAME2, &col_num2, &row_num2);
    int row_size = (row_num1 + row_num2) / NR_DPUS;

    // Set test_array
    load_csv(FILE_NAME1, col_num1, row_num1, &test_array1);
    load_csv(FILE_NAME2, col_num2, row_num2, &test_array2);

    int using_dpus;
    if (row_size == 0)
    {
        using_dpus = 2;
        row_size = row_num1;
    }
    else
    {
        using_dpus = NR_DPUS;
    }

    // Allocate DPUs
    struct dpu_set_t set, dpu;
    uint32_t dpu_id;
    DPU_ASSERT(dpu_alloc(using_dpus, "backend=simulator", &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY_SELECT, NULL));

    // Set input arguments
    dpu_block_t input_args[using_dpus * 2];
    int temp_first_row = row_num1;
    int temp_second_row = row_num2;
    for (int i = 0; i < using_dpus - 1; i++)
    {
        if (temp_first_row > 0)
        {
            input_args[i].table_num = 0;
            input_args[i].col_num = col_num1;
            if (temp_first_row > row_size)
            {
                input_args[i].row_num = row_size;
                temp_first_row -= row_size;
            }
            else
            {
                input_args[i].row_num = temp_first_row;
                temp_first_row = 0;
                pivot_id = i + 1;
            }
        }
        else
        {
            input_args[i].table_num = 1;
            input_args[i].col_num = col_num2;
            if (temp_second_row >= row_size)
            {
                input_args[i].row_num = row_size;
                temp_second_row -= row_size;
            }
            else
            {
                input_args[i].row_num = temp_second_row;
                temp_second_row = 0;
            }
        }
    }
    input_args[using_dpus - 1].table_num = 1;
    input_args[using_dpus - 1].col_num = col_num2;
    input_args[using_dpus - 1].row_num = temp_second_row;

    // Transfer input arguments and test_array to DPUs
    start(&timer, 0, 0);
    DPU_FOREACH(set, dpu, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &input_args[dpu_id]));
    }
    DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));

    DPU_FOREACH(set, dpu, dpu_id)
    {
        int transfer_size = input_args[dpu_id].row_num * input_args[dpu_id].col_num * sizeof(T);
        if (input_args[dpu_id].table_num == 0)
        {
            int offset = dpu_id * row_size * col_num1;
            DPU_ASSERT(dpu_prepare_xfer(dpu, test_array1 + offset));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
        }
        else
        {
            int offset = (dpu_id - pivot_id) * row_size * col_num2;
            DPU_ASSERT(dpu_prepare_xfer(dpu, test_array2 + offset));
            DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
        }
    }
    stop(&timer, 0);

    start(&timer, 1, 0);
    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));
    stop(&timer, 1);

    // Retrieve dpu_result from DPUs
    start(&timer, 2, 0);
    DPU_FOREACH(set, dpu, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &input_args[dpu_id]));
        DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));
        dpu_result[dpu_id].table_num = input_args[dpu_id].table_num;
        dpu_result[dpu_id].dpu_id = dpu_id;
        dpu_result[dpu_id].col_num = input_args[dpu_id].col_num;
        dpu_result[dpu_id].row_num = input_args[dpu_id].row_num;
        if (dpu_result[dpu_id].table_num == 0)
            total_row_num1 += dpu_result[dpu_id].row_num;
        else
            total_row_num2 += dpu_result[dpu_id].row_num;

        int transfer_size = dpu_result[dpu_id].row_num * dpu_result[dpu_id].col_num * sizeof(T);
        dpu_result[dpu_id].arr = (T *)malloc(transfer_size);
        DPU_ASSERT(dpu_prepare_xfer(dpu, dpu_result[dpu_id].arr));
        DPU_ASSERT(dpu_push_xfer(set, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    }
    stop(&timer, 2);

    T *select_array1 = (T *)malloc(col_num1 * total_row_num1 * sizeof(T));
    int offset = 0;
    for (int i = 0; i < pivot_id; i++)
    {
        int size = dpu_result[i].row_num * dpu_result[i].col_num;
        memcpy(select_array1 + offset, dpu_result[i].arr, size * sizeof(T));
        offset += size;
    }

    T *select_array2 = (T *)malloc(col_num2 * total_row_num2 * sizeof(T));
    offset = 0;
    for (int i = pivot_id; i < using_dpus; i++)
    {
        int size = dpu_result[i].row_num * dpu_result[i].col_num;
        memcpy(select_array2 + offset, dpu_result[i].arr, size * sizeof(T));
        offset += size;
    }

    cpu_dpu_time += (timer.time[0] / 1000);
    dpu_time += (timer.time[1] / 1000);
    dpu_cpu_time += (timer.time[2] / 1000);

#ifdef DEBUG
    printf("==================\n");
    printf("#    select.c    #\n");
    printf("==================\n");

    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

    printf("####################\n\n");
#endif

    DPU_ASSERT(dpu_free(set));

    /* ************************ */
    /*     sort in each DPU     */
    /* ************************ */

    // Allocate DPUs
    struct dpu_set_t set1, dpu1;
    DPU_ASSERT(dpu_alloc(using_dpus, "backend=simulator", &set1));
    DPU_ASSERT(dpu_load(set1, DPU_BINARY_SORT_DPU, NULL));

    // Set input arguments
    row_size = total_row_num1 / pivot_id;
    for (int i = 0; i < pivot_id - 1; i++)
    {
        input_args[i].col_num = col_num1;
        input_args[i].row_num = row_size;
        dpu_result[i].row_num = row_size;
    }
    input_args[pivot_id - 1].col_num = col_num1;
    input_args[pivot_id - 1].row_num = total_row_num1 - (pivot_id - 1) * row_size;
    dpu_result[pivot_id - 1].row_num = total_row_num1 - (pivot_id - 1) * row_size;

    int temp_row_size = total_row_num2 / (using_dpus - pivot_id);
    for (int i = pivot_id; i < using_dpus - 1; i++)
    {
        input_args[i].col_num = col_num2;
        input_args[i].row_num = temp_row_size;
        dpu_result[i].row_num = temp_row_size;
    }
    input_args[using_dpus - 1].col_num = col_num2;
    input_args[using_dpus - 1].row_num = total_row_num2 - (using_dpus - pivot_id - 1) * temp_row_size;
    dpu_result[using_dpus - 1].row_num = total_row_num2 - (using_dpus - pivot_id - 1) * temp_row_size;

    // Transfer input arguments and test_array to DPUs
    start(&timer, 0, 0);
    DPU_FOREACH(set1, dpu1, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu1, &input_args[dpu_id]));
    }
    DPU_ASSERT(dpu_push_xfer(set1, DPU_XFER_TO_DPU, "bl", 0, sizeof(input_args[0]), DPU_XFER_DEFAULT));

    DPU_FOREACH(set1, dpu1, dpu_id)
    {
        int transfer_size = input_args[dpu_id].row_num * input_args[dpu_id].col_num * sizeof(T);
        dpu_result[dpu_id].arr = (T *)malloc(transfer_size);
        if (input_args[dpu_id].table_num == 0)
            DPU_ASSERT(dpu_prepare_xfer(dpu1, select_array1 + dpu_id * row_size * col_num1));
        else
            DPU_ASSERT(dpu_prepare_xfer(dpu1, select_array2 + (dpu_id - pivot_id) * temp_row_size * col_num2));
        DPU_ASSERT(dpu_push_xfer(set1, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    }
    stop(&timer, 0);

    start(&timer, 1, 0);
    DPU_ASSERT(dpu_launch(set1, DPU_SYNCHRONOUS));
    stop(&timer, 1);

    // Retrieve dpu_result from DPUs
    start(&timer, 2, 0);
    DPU_FOREACH(set1, dpu1, dpu_id)
    {
        int transfer_size = input_args[dpu_id].row_num * input_args[dpu_id].col_num * sizeof(T);
        DPU_ASSERT(dpu_prepare_xfer(dpu1, dpu_result[dpu_id].arr));
        DPU_ASSERT(dpu_push_xfer(set1, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
    }
    stop(&timer, 2);

    cpu_dpu_time += (timer.time[0] / 1000);
    dpu_time += (timer.time[1] / 1000);
    dpu_cpu_time += (timer.time[2] / 1000);

#ifdef DEBUG
    printf("==================\n");
    printf("#     sort.c     #\n");
    printf("==================\n");

    for (int d = 0; d < using_dpus; d++)
    {
        printf("Table %d - DPU %d sort %d rows\n", dpu_result[d].table_num, dpu_result[d].dpu_id, dpu_result[d].row_num);
        // for (int i = 0; i < dpu_result[d].row_num; i++)
        // {
        //     for (int j = 0; j < dpu_result[d].col_num; j++)
        //     {
        //         printf("%ld ", dpu_result[d].arr[i * dpu_result[d].col_num + j]);
        //     }
        //     printf("\n");
        // }
        // printf("---------------\n");
    }

    printf("total_row_num: %d %d\n", total_row_num1, total_row_num2);
    printf("####################\n\n");
#endif

    DPU_ASSERT(dpu_free(set1));
    free(test_array1);
    free(test_array2);
    free(select_array1);
    free(select_array2);

    /* ****************************** */
    /*     add & sort DPU results     */
    /* ****************************** */

    // merge each dpu results
    struct dpu_set_t set2, dpu2;
    int cnt = 0;
    int cur_dpus = pivot_id;
    int cur_dpus2 = using_dpus - pivot_id;
    bool check[2] = {false}; // completion check for two tables

    while (!check[0] || !check[1])
    {
        int next = (cur_dpus + 1) / 2 + (cur_dpus2 + 1) / 2;
        DPU_ASSERT(dpu_alloc(next, "backend=simulator", &set2));
        DPU_ASSERT(dpu_load(set2, DPU_BINARY_MERGE_DPU, NULL));

        // Transfer input arguments and test_array to DPUs
        start(&timer, 0, 0);
        DPU_FOREACH(set2, dpu2, dpu_id)
        {
            int table_num, pair_index, temp_cur;
            if (dpu_id < (cur_dpus + 1) / 2)
            {
                table_num = 0;
                pair_index = dpu_id * 2;
                temp_cur = cur_dpus;
            }
            else
            {
                table_num = 1;
                pair_index = pivot_id + (dpu_id - (cur_dpus + 1) / 2) * 2;
                temp_cur = pivot_id + cur_dpus2;
            }
            bool is_checked = check[table_num];

            if (pair_index + 1 < temp_cur && !is_checked)
            {
                DPU_ASSERT(dpu_prepare_xfer(dpu2, &input_args[pair_index]));
                DPU_ASSERT(dpu_push_xfer(set2, DPU_XFER_TO_DPU, "bl1", 0, sizeof(dpu_block_t), DPU_XFER_DEFAULT));
                DPU_ASSERT(dpu_prepare_xfer(dpu2, &input_args[pair_index + 1]));
                DPU_ASSERT(dpu_push_xfer(set2, DPU_XFER_TO_DPU, "bl2", 0, sizeof(dpu_block_t), DPU_XFER_DEFAULT));

                uint32_t first_size = input_args[pair_index].row_num * input_args[pair_index].col_num * sizeof(T);
                uint32_t second_size = input_args[pair_index + 1].row_num * input_args[pair_index + 1].col_num * sizeof(T);
                DPU_ASSERT(dpu_prepare_xfer(dpu2, dpu_result[pair_index].arr));
                DPU_ASSERT(dpu_push_xfer(set2, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, first_size, DPU_XFER_DEFAULT));
                DPU_ASSERT(dpu_prepare_xfer(dpu2, dpu_result[pair_index + 1].arr));
                DPU_ASSERT(dpu_push_xfer(set2, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, first_size + second_size, second_size, DPU_XFER_DEFAULT));

                free(dpu_result[pair_index].arr);
                free(dpu_result[pair_index + 1].arr);
            }
        }
        stop(&timer, 0);

        start(&timer, 1, 0);
        DPU_ASSERT(dpu_launch(set2, DPU_SYNCHRONOUS));
        stop(&timer, 1);

        // Retrieve dpu_result from DPUs
        start(&timer, 2, 0);
        DPU_FOREACH(set2, dpu2, dpu_id)
        {
            int table_num, pair_index, temp_cur, temp_dpu_id;
            if (dpu_id < (cur_dpus + 1) / 2)
            {
                table_num = 0;
                pair_index = dpu_id * 2;
                temp_cur = cur_dpus;
                temp_dpu_id = dpu_id;
            }
            else
            {
                table_num = 1;
                pair_index = pivot_id + (dpu_id - (cur_dpus + 1) / 2) * 2;
                temp_cur = pivot_id + cur_dpus2;
                temp_dpu_id = pivot_id + dpu_id - (cur_dpus + 1) / 2;
            }
            bool is_checked = check[table_num];

            if (pair_index + 1 < temp_cur && !is_checked)
            {
                int transfer_size = (input_args[pair_index].row_num + input_args[pair_index + 1].row_num) * input_args[pair_index].col_num * sizeof(T);

                dpu_result[temp_dpu_id].arr = (T *)malloc(transfer_size);
                dpu_result[temp_dpu_id].row_num = input_args[pair_index].row_num + input_args[pair_index + 1].row_num;
                input_args[temp_dpu_id].row_num = input_args[pair_index].row_num + input_args[pair_index + 1].row_num;

                DPU_ASSERT(dpu_prepare_xfer(dpu2, dpu_result[temp_dpu_id].arr));
                DPU_ASSERT(dpu_push_xfer(set2, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, transfer_size, DPU_XFER_DEFAULT));
            }
        }
        stop(&timer, 2);

        DPU_ASSERT(dpu_free(set2));

        if (cur_dpus % 2 == 1)
        {
            int target_dpu = cur_dpus / 2;

            input_args[target_dpu] = input_args[cur_dpus - 1];
            dpu_result[target_dpu] = dpu_result[cur_dpus - 1];
            dpu_result[target_dpu].dpu_id = target_dpu;
        }
        if (cur_dpus2 % 2 == 1)
        {
            int target_dpu = cur_dpus2 / 2 + pivot_id;

            input_args[target_dpu] = input_args[cur_dpus2 - 1 + pivot_id];
            dpu_result[target_dpu] = dpu_result[cur_dpus2 - 1 + pivot_id];
            dpu_result[target_dpu].dpu_id = target_dpu;
        }

        if (!check[0])
        {
            cur_dpus = (cur_dpus + 1) / 2;
        }
        if (!check[1])
        {
            cur_dpus2 = (cur_dpus2 + 1) / 2;
        }

        if (cur_dpus <= 1)
        {
            cur_dpus = 0;
            check[0] = true;
        }
        if (cur_dpus2 <= 1)
        {
            cur_dpus2 = 0;
            check[1] = true;
        }

        cnt++;

        cpu_dpu_time += (timer.time[0] / 1000);
        dpu_time += (timer.time[1] / 1000);
        dpu_cpu_time += (timer.time[2] / 1000);
    }

#ifdef DEBUG
    printf("==================\n");
    printf("#     merge.c    #\n");
    printf("==================\n");

    printf("Table 0 %d rows\n", dpu_result[0].row_num);
    // for (int i = 0; i < dpu_result[0].row_num; i++)
    // {
    //     for (int j = 0; j < dpu_result[0].col_num; j++)
    //     {
    //         printf("%ld ", dpu_result[0].arr[i * dpu_result[0].col_num + j]);
    //     }
    //     printf("\n");
    // }

    printf("Table 1 %d rows\n", dpu_result[pivot_id].row_num);
    // for (int i = 0; i < dpu_result[pivot_id].row_num; i++)
    // {
    //     for (int j = 0; j < dpu_result[pivot_id].col_num; j++)
    //     {
    //         printf("%ld ", dpu_result[pivot_id].arr[i * dpu_result[pivot_id].col_num + j]);
    //     }
    //     printf("\n");
    // }
    // printf("---------------\n");

    printf("total row num: %d %d\n", total_row_num1, total_row_num2);
    printf("####################\n\n");
#endif

    /* ************************** */
    /*     join dpu_result[0]     */
    /*             &              */
    /*    dpu_result[pivot_id]    */
    /* ************************** */

    int used_idx[pivot_id];
    int cur_idx_t2 = 0;

    // Set input arguments
    row_size = total_row_num1 / pivot_id;

    for (int i = 0; i < pivot_id - 1; i++)
    {
        input_args[i].col_num = col_num1;
        input_args[i].row_num = row_size;
        dpu_result[i].row_num = row_size;

        if (i)
        {
            dpu_result[i].arr = malloc(col_num1 * row_size * sizeof(T));
            memcpy(dpu_result[i].arr, dpu_result[0].arr + (col_num1 * row_size) * i, col_num1 * row_size * sizeof(T));
        }

        used_idx[i] = binary_search(&dpu_result[pivot_id], JOIN_KEY2, dpu_result[i].arr[(row_size - 1) * col_num1 + JOIN_KEY1]);

        if (i)
        {
            input_args[pivot_id + i].col_num = col_num2;
            input_args[pivot_id + i].row_num = used_idx[i] - cur_idx_t2 + 1;
            dpu_result[pivot_id + i].row_num = used_idx[i] - cur_idx_t2 + 1;
            dpu_result[pivot_id + i].arr = malloc(dpu_result[pivot_id + i].row_num * col_num2 * sizeof(T));
            memcpy(dpu_result[pivot_id + i].arr, dpu_result[pivot_id].arr + cur_idx_t2 * col_num2, dpu_result[pivot_id + i].row_num * col_num2 * sizeof(T));
        }

        cur_idx_t2 = used_idx[i] + 1;
    }

    input_args[pivot_id - 1].col_num = col_num1;
    input_args[pivot_id - 1].row_num = total_row_num1 - (pivot_id - 1) * row_size;
    dpu_result[pivot_id - 1].row_num = total_row_num1 - (pivot_id - 1) * row_size;

    dpu_result[pivot_id - 1].arr = malloc(col_num1 * (total_row_num1 - (pivot_id - 1) * row_size) * sizeof(T));
    memcpy(dpu_result[pivot_id - 1].arr, dpu_result[0].arr + col_num1 * row_size * (pivot_id - 1), col_num1 * (total_row_num1 - (pivot_id - 1) * row_size) * sizeof(T));

    input_args[2 * pivot_id - 1].col_num = col_num2;
    input_args[2 * pivot_id - 1].row_num = total_row_num2 - cur_idx_t2 + 1;
    dpu_result[2 * pivot_id - 1].row_num = total_row_num2 - cur_idx_t2 + 1;

    dpu_result[2 * pivot_id - 1].arr = malloc(dpu_result[2 * pivot_id - 1].row_num * col_num2 * sizeof(T));
    memcpy(dpu_result[2 * pivot_id - 1].arr, dpu_result[pivot_id].arr + cur_idx_t2 * col_num2, dpu_result[2 * pivot_id - 1].row_num * col_num2 * sizeof(T));

    input_args[pivot_id].col_num = col_num2;
    input_args[pivot_id].row_num = used_idx[0] + 1;
    dpu_result[pivot_id].row_num = used_idx[0] + 1;

    // Transfer input arguments and test_array to DPUs
    struct dpu_set_t set3, dpu3;

    DPU_ASSERT(dpu_alloc(pivot_id, "backend=simulator", &set3));
    DPU_ASSERT(dpu_load(set3, DPU_BINARY_JOIN, NULL));

    start(&timer, 3, 0);
    DPU_FOREACH(set3, dpu3, dpu_id)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu3, &input_args[dpu_id]));
        DPU_ASSERT(dpu_push_xfer(set3, DPU_XFER_TO_DPU, "bl1", 0, sizeof(dpu_block_t), DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_prepare_xfer(dpu3, &input_args[pivot_id + dpu_id]));
        DPU_ASSERT(dpu_push_xfer(set3, DPU_XFER_TO_DPU, "bl2", 0, sizeof(dpu_block_t), DPU_XFER_DEFAULT));

        uint32_t first_size = input_args[dpu_id].row_num * input_args[dpu_id].col_num * sizeof(T);
        uint32_t second_size = input_args[pivot_id + dpu_id].row_num * input_args[pivot_id + dpu_id].col_num * sizeof(T);
        DPU_ASSERT(dpu_prepare_xfer(dpu3, dpu_result[dpu_id].arr));
        DPU_ASSERT(dpu_push_xfer(set3, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, 0, first_size, DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_prepare_xfer(dpu3, dpu_result[pivot_id + dpu_id].arr));
        DPU_ASSERT(dpu_push_xfer(set3, DPU_XFER_TO_DPU, DPU_MRAM_HEAP_POINTER_NAME, first_size, second_size, DPU_XFER_DEFAULT));

        free(dpu_result[dpu_id].arr);
        free(dpu_result[pivot_id + dpu_id].arr);
    }
    stop(&timer, 3);

    start(&timer, 1, 0);
    DPU_ASSERT(dpu_launch(set3, DPU_SYNCHRONOUS));
    stop(&timer, 1);

    // Retrieve dpu_result from DPUs
    T *result[using_dpus];
    int cur_idx = 0;
    int joined_row[using_dpus];

    start(&timer, 2, 0);
    DPU_FOREACH(set3, dpu3, dpu_id)
    {
        uint32_t first_size = input_args[dpu_id].row_num * col_num1 * sizeof(T);
        uint32_t second_size = input_args[pivot_id + dpu_id].row_num * col_num2 * sizeof(T);

        DPU_ASSERT(dpu_prepare_xfer(dpu3, &joined_row[dpu_id]));
        DPU_ASSERT(dpu_push_xfer(set3, DPU_XFER_FROM_DPU, "joined_row", 0, sizeof(int), DPU_XFER_DEFAULT));

        result[dpu_id] = (T *)malloc(joined_row[dpu_id] * (col_num1 + col_num2 - 1) * sizeof(T));

        uint64_t size = joined_row[dpu_id] * (col_num1 + col_num2 - 1) * sizeof(T);

        DPU_ASSERT(dpu_prepare_xfer(dpu3, result[dpu_id]));
        DPU_ASSERT(dpu_push_xfer(set3, DPU_XFER_FROM_DPU, DPU_MRAM_HEAP_POINTER_NAME, first_size + second_size, size, DPU_XFER_DEFAULT));

        cur_idx += joined_row[dpu_id];
    }
    stop(&timer, 2);

    cpu_dpu_time += (timer.time[3] / 1000);
    dpu_time += (timer.time[1] / 1000);
    dpu_cpu_time += (timer.time[2] / 1000);

#ifdef DEBUG
    printf("==================\n");
    printf("#     join.c     #\n");
    printf("==================\n");

    printf("Rows: %u\n", cur_idx);
    printf("COL NUM 1: %d / COL NUM 2: %d\n", col_num1, col_num2);

    for (int d = 0; d < pivot_id; d++)
    {
        printf("DPU %d results: %d rows\n", dpu_result[d].dpu_id, joined_row[d]);
        // for (int i = 0; i < joined_row[d]; i++)
        // {
        //     for (int j = 0; j < col_num1 + col_num2 - 1; j++)
        //     {
        //         printf("%ld ", result[d][i * (col_num1 + col_num2 - 1) + j]);
        //     }
        //     printf("\n");
        // }
        // printf("---------------\n");
    }

    printf("####################\n\n");
#endif

    // save result as csv file
    FILE *file = fopen("./data/result.csv", "w");
    if (!file)
    {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }

    int total_col = col_num1 + col_num2 - 1;

    for (int i = 1; i <= total_col; i++)
    {
        fprintf(file, "col%d", i);
        if (i < total_col)
        {
            fprintf(file, ",");
        }
    }
    fprintf(file, "\n");

    for (int d = 0; d < pivot_id; d++)
    {
        for (int i = 0; i < joined_row[d]; i++)
        {
            for (int j = 0; j < total_col; j++)
            {
                fprintf(file, "%ld", result[d][i * (col_num1 + col_num2 - 1) + j]);
                if (j < total_col - 1)
                {
                    fprintf(file, ",");
                }
            }
            fprintf(file, "\n");
        }
    }

    fclose(file);

    for (int d = 0; d < pivot_id; d++)
    {
        free(result[d]);
    }
    DPU_ASSERT(dpu_free(set3));

    printf("\n");
    printf("######### PIM #########\n");
    printf("### SORT-MERGE-JOIN ###\n");
    printf("         EXEC TIME     \n");
    printf("CPU-DPU  %f\n", cpu_dpu_time);
    printf("DPU      %f\n", dpu_time);
    printf("DPU-CPU  %f\n", dpu_cpu_time);
    printf("-----------------------\n");
    printf("TOTAL %f\n", cpu_dpu_time + dpu_time + dpu_cpu_time);
    printf("#######################\n\n");

    return 0;
}
