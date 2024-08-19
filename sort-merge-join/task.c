// dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o task task.c

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>
#include <string.h>
#include <mram.h>
#include <alloc.h>
#include "common.h"

#define UNDEFINED_VAL (-1)
int shared_var = UNDEFINED_VAL;

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host int col_num;
__host int row_num;
__mram_noinit int test_array[MAX_ROW * MAX_COL];
__mram_noinit dpu_result result_array;

tasklet_result result[NR_TASKLETS];
int result_size = NR_TASKLETS;
bool check[NR_TASKLETS] = {false};

int simple_ceil(double value)
{
    int int_part = (int)value;
    return (value > int_part) ? int_part + 1 : int_part;
}

bool is_check_true(bool *check, int size)
{
    for (int i = 0; i < (size - 1) / 2; i++)
    {
        if (!check[i])
            return false;
    }
    return true;
}

// TODO: Replace with quick sort
void bubble_sort(int *arr, int row_num, int col_num, int key)
{
    for (int i = 0; i < row_num - 1; i++)
    {
        for (int j = 0; j < row_num - i - 1; j++)
        {
            if (arr[j * col_num + key] > arr[(j + 1) * col_num + key])
            {
                for (int k = 0; k < col_num; k++)
                {
                    int temp = arr[j * col_num + k];
                    arr[j * col_num + k] = arr[(j + 1) * col_num + k];
                    arr[(j + 1) * col_num + k] = temp;
                }
            }
        }
    }
}

void merge_in_asc(tasklet_result *a, tasklet_result *b, int col_num, int key)
{
    int a_idx = 0;
    int b_idx = 0;
    int a_size = a->row_num;
    int b_size = b->row_num;
    int *a_arr = a->arr;
    int *b_arr = b->arr;

    int *merged_arr = (int *)mem_alloc((a_size + b_size) * col_num * sizeof(int));
    int *merged_idx = merged_arr;

    while (a_idx < a_size && b_idx < b_size)
    {
        if (a_arr[a_idx * col_num + key] <= b_arr[b_idx * col_num + key])
        {
            for (int i = 0; i < col_num; i++)
                *(merged_idx + i) = *(a_arr + a_idx * col_num + i);
            a_idx++;
        }
        else
        {
            for (int i = 0; i < col_num; i++)
                *(merged_idx + i) = *(b_arr + b_idx * col_num + i);
            b_idx++;
        }
        merged_idx += col_num;
    }

    while (a_idx < a_size)
    {
        for (int i = 0; i < col_num; i++)
            *(merged_idx + i) = *(a_arr + a_idx * col_num + i);
        a_idx++;
        merged_idx += col_num;
    }

    while (b_idx < b_size)
    {
        for (int i = 0; i < col_num; i++)
            *(merged_idx + i) = *(b_arr + b_idx * col_num + i);
        b_idx++;
        merged_idx += col_num;
    }

    a->row_num = a_size + b_size;
    a->arr = merged_arr;
}

int main()
{
    // -------------------- Allocate --------------------

    int row_per_tasklet = row_num / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num;
    unsigned int tasklet_id = me();
    int start = tasklet_id * chunk_size;
    __mram_ptr int *tasklet_test_array = &test_array[start];

    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    mutex_lock(my_mutex); // rm
    printf("Tasklet %d is running\n", tasklet_id);
    for (int i = 0; i < chunk_size / col_num; i++)
    {
        for (int j = 0; j < col_num; j++)
            printf("%d ", *(tasklet_test_array + i * col_num + j));
        printf("\n");
    }
    printf("\n");

    // -------------------- Select & Sort --------------------

    __mram_ptr int *index = &tasklet_test_array[0];
    int cur_num = 0;

    for (int i = 0; i < row_per_tasklet; i++)
    {
        if (*(index + col_num * i + SELECT_COL) > SELECT_VAL)
            cur_num++;
    }

    int *selected_array = (int *)mem_alloc(chunk_size * sizeof(int));
    int *selected_idx = selected_array;

    for (int i = 0; i < row_per_tasklet; i++)
    {
        if (*(index + SELECT_COL) > SELECT_VAL)
        {
            for (int c = 0; c < col_num; c++)
                *(selected_idx + c) = *(index + c);
            selected_idx += col_num;
        }
        index += col_num;
    }

    bubble_sort(selected_array, cur_num, col_num, JOIN_KEY);

    printf("Select and sort result:\n");
    for (int i = 0; i < cur_num; i++)
    {
        for (int j = 0; j < col_num; j++)
            printf("%d ", *(selected_array + i * col_num + j));
        printf("\n");
    }
    printf("---------------\n");

    mutex_unlock(my_mutex); // rm
    barrier_wait(&my_barrier);

    // -------------------- Save --------------------

    // mutex_lock(my_mutex);
    result[tasklet_id].tasklet_id = tasklet_id;
    result[tasklet_id].row_num = cur_num;
    result[tasklet_id].arr = (int *)mem_alloc(cur_num * col_num * sizeof(int));
    for (int i = 0; i < cur_num; i++)
    {
        for (int j = 0; j < col_num; j++)
        {
            result[tasklet_id].arr[i * col_num + j] = selected_array[i * col_num + j];
        }
    }
    // mutex_unlock(my_mutex);
    barrier_wait(&my_barrier);

    // -------------------- Merge --------------------

    while (result_size != 1)
    {
        if (tasklet_id <= result_size / 2 - 1 && !check[tasklet_id])
        {
            // mutex_lock(my_mutex);
            merge_in_asc(&result[tasklet_id], &result[result_size - 1 - tasklet_id], col_num, JOIN_KEY);
            check[tasklet_id] = true;
            if (is_check_true(check, result_size))
            {
                result_size = simple_ceil(result_size / 2.0);
                memset(check, 0, sizeof(check));
            }
            // mutex_unlock(my_mutex);
        }

        barrier_wait(&my_barrier);
    }

    // barrier_wait(&my_barrier);

    if (tasklet_id == NR_TASKLETS - 1)
    {
        printf("Merge result:\n");
        for (int i = 0; i < result[0].row_num; i++)
        {
            for (int j = 0; j < col_num; j++)
                printf("%d ", result[0].arr[i * col_num + j]);
            printf("\n");
        }

        result_array.row_num = result[0].row_num;
        for (int i = 0; i < result[0].row_num * col_num; i++)
            result_array.arr[i] = result[0].arr[i];
    }

    mem_reset();

    return 0;
}