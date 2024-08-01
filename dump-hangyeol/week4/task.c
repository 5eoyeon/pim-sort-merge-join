// // dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -O2 -o task task.c

#include <stdio.h>
#include <barrier.h>
#include <defs.h>
#include <stdint.h>
#include <string.h>
#include <mutex.h>

#define NR_TASKLETS 2

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host int col_num;
__host int row_num;
__mram_noinit int test_array[100];
__mram_noinit int tasklet_array[100];

void bubble_sort(__mram_ptr int *arr, int row_num, int col_num, int key)
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

void copy_data(__mram_ptr int *dest, __mram_ptr int *src, int size)
{
    for (int i = 0; i < size; i++)
    {
        dest[i] = src[i];
    }
}

int main()
{
    int chunk_size = (row_num / NR_TASKLETS) * col_num;
    unsigned int tasklet_id = me();
    int start = tasklet_id * chunk_size;
    __mram_ptr int *tasklet_test_array = &test_array[start];
    if (tasklet_id == NR_TASKLETS - 1)
        chunk_size += (col_num * row_num) - NR_TASKLETS * chunk_size;

    bubble_sort(tasklet_test_array, chunk_size / col_num, col_num, 0); // col0을 기준으로 bubble sort

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

    barrier_wait(&my_barrier);

    // Tasklet 1의 데이터를 tasklet 0으로 복사
    if (tasklet_id == 1)
    {
        copy_data(&test_array[start], tasklet_test_array, chunk_size);
    }

    barrier_wait(&my_barrier);

    // Tasklet 0에서 전체 데이터 정렬
    if (tasklet_id == 0)
    {
        bubble_sort(test_array, row_num, col_num, 0);

        mutex_lock(my_mutex);
        printf("Final sorted array:\n");
        for (int i = 0; i < row_num; i++)
        {
            for (int j = 0; j < col_num; j++)
                printf("%d ", test_array[i * col_num + j]);
            printf("\n");
        }
        printf("\n");
        mutex_unlock(my_mutex);
    }

    return 0;
}
