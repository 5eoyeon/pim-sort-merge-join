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

__host dpu_block_t bl;

tasklet_result_t result[NR_TASKLETS];
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

void quick_sort(int *arr, int row_num, int col_num, int key)
{
    if (row_num <= 1)
        return;

    int pivot = arr[(row_num / 2) * col_num + key];
    int i = 0;
    int j = row_num - 1;

    while (i <= j)
    {
        while (arr[i * col_num + key] < pivot)
            i++;
        while (arr[j * col_num + key] > pivot)
            j--;

        if (i <= j)
        {
            for (int k = 0; k < col_num; k++)
            {
                int temp = arr[i * col_num + k];
                arr[i * col_num + k] = arr[j * col_num + k];
                arr[j * col_num + k] = temp;
            }
            i++;
            j--;
        }
    }

    quick_sort(arr, j + 1, col_num, key);
    quick_sort(arr + i * col_num, row_num - i, col_num, key);
}

void merge_in_asc(tasklet_result_t *a, tasklet_result_t *b, int col_num, int key)
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

    unsigned int tasklet_id = me();
    int col_num = bl.col_num;
    int row_num = bl.row_num;

    int row_per_tasklet = row_num / NR_TASKLETS;
    int chunk_size = row_per_tasklet * col_num;
    int start = tasklet_id * chunk_size;

    if (tasklet_id == NR_TASKLETS - 1)
    {
        row_per_tasklet = row_num - (NR_TASKLETS - 1) * row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    int *tasklet_test_array = (int *)mem_alloc(chunk_size * sizeof(int));
    uint32_t mram_base_addr = (uint32_t)DPU_MRAM_HEAP_POINTER;
    mram_read((__mram_ptr void const *)(mram_base_addr + start * sizeof(int)), tasklet_test_array, chunk_size * sizeof(int));

#ifdef DEBUG
    mutex_lock(my_mutex);
    printf("Tasklet %d is running\n", tasklet_id);
    for (int i = 0; i < chunk_size / col_num; i++)
    {
        for (int j = 0; j < col_num; j++)
            printf("%d ", *(tasklet_test_array + i * col_num + j));
        printf("\n");
    }
    printf("\n");
    mutex_unlock(my_mutex);
#endif

    // -------------------- Select & Sort --------------------

    int *index = &tasklet_test_array[0];
    int cur_num = 0;

    for (int i = 0; i < row_per_tasklet; i++)
    {
        if (*(index + col_num * i + JOIN_COL) > JOIN_VAL)
            cur_num++;
    }

    int *selected_array = (int *)mem_alloc(chunk_size * sizeof(int));
    int *selected_idx = selected_array;

    for (int i = 0; i < row_per_tasklet; i++)
    {
        if (*(index + JOIN_COL) > JOIN_VAL)
        {
            for (int c = 0; c < col_num; c++)
                *(selected_idx + c) = *(index + c);
            selected_idx += col_num;
        }
        index += col_num;
    }

    quick_sort(selected_array, cur_num, col_num, JOIN_KEY);
    barrier_wait(&my_barrier);

#ifdef DEBUG
    mutex_lock(my_mutex);
    printf("Select and sort in Tasklet %d:\n", tasklet_id);
    for (int i = 0; i < cur_num; i++)
    {
        for (int j = 0; j < col_num; j++)
            printf("%d ", *(selected_array + i * col_num + j));
        printf("\n");
    }
    printf("\n");
    mutex_unlock(my_mutex);
#endif

    // -------------------- Save --------------------

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
    barrier_wait(&my_barrier);

    // -------------------- Merge --------------------

    while (result_size != 1)
    {
        if (tasklet_id <= result_size / 2 - 1 && !check[tasklet_id])
        {
            merge_in_asc(&result[tasklet_id], &result[result_size - 1 - tasklet_id], col_num, JOIN_KEY);
            check[tasklet_id] = true;
            if (is_check_true(check, result_size))
            {
                result_size = simple_ceil(result_size / 2.0);
                memset(check, 0, sizeof(check));
            }
        }
        barrier_wait(&my_barrier);
    }

    if (tasklet_id == NR_TASKLETS - 1)
    {
#ifdef DEBUG
        mutex_lock(my_mutex);
        printf("Merge result:\n");
        for (int i = 0; i < result[0].row_num; i++)
        {
            for (int j = 0; j < col_num; j++)
                printf("%d ", result[0].arr[i * col_num + j]);
            printf("\n");
        }
        printf("\n");
        mutex_unlock(my_mutex);
#endif

        bl.col_num = col_num;
        bl.row_num = result[0].row_num;
        int transfer_size = result[0].row_num * col_num * sizeof(int);
        mram_write(result[0].arr, (__mram_ptr void *)(mram_base_addr), transfer_size);
    }
    mem_reset();

    return 0;
}