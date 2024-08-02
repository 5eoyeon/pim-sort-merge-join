// dpu-upmem-dpurte-clang -DNR_TASKLETS=2 -o task task.c

#include <stdio.h>
#include <defs.h>
#include <barrier.h>
#include <mutex.h>
#include <stdint.h>

#define NR_TASKLETS 2
#define MAX_COL 10
#define MAX_ROW 10

#define UNDEFINED_VAL (-1)
int shared_var = UNDEFINED_VAL;

BARRIER_INIT(my_barrier, NR_TASKLETS);
MUTEX_INIT(my_mutex);

__host int col_num;
__host int row_num;
__host int join_col;
__host int join_val;
__mram_noinit int test_array[MAX_ROW * MAX_COL];

int main() {
    int row_per_tasklet = row_num / NR_TASKLETS;
    int chunk_size = row_per_tasklet*col_num;
    unsigned int tasklet_id = me();
    int start = tasklet_id * chunk_size;
    __mram_ptr int* tasklet_test_array = &test_array[start];

    if(tasklet_id == NR_TASKLETS-1) {
        row_per_tasklet = row_num - (NR_TASKLETS-1)*row_per_tasklet;
        chunk_size = row_per_tasklet * col_num;
    }

    mutex_lock(my_mutex);
    printf("Tasklet %d is running\n", tasklet_id);
    for(int i = 0; i < chunk_size / col_num; i++) {
        for(int j = 0; j < col_num; j++) printf("%d ", *(tasklet_test_array + i*col_num + j));
        printf("\n");
    }
    printf("\n");

    //select (in col 2, val 5)
    int join_col = 2;
    int join_val = 5;
    __mram_ptr int* index = &tasklet_test_array[0];
    int cur_num = 0;
    
    for(int i = 0; i < row_per_tasklet; i++) {
        if(*(index + col_num * i + join_col) > join_val) cur_num++;
    }
    
    int* selected_array = (int*) mem_alloc(chunk_size * sizeof(int));
    int* selected_idx = selected_array;
    
    for(int i = 0; i < row_per_tasklet; i++) {
        if(*(index + 2) > 5) {
            for(int c = 0; c < col_num; c++) *(selected_idx + c) = *(index + c);
            selected_idx += col_num;
        }
        index += col_num;
    }

    for(int i = 0; i < cur_num; i++) {
        for(int j = 0; j < col_num; j++) printf("%d ", *(selected_array + i * col_num + j));
        printf("\n");
    }
    printf("\n");

    mutex_unlock(my_mutex); // will be changed
    barrier_wait(&my_barrier);

    int* sorted_array = (int*) mem_alloc(chunk_size * sizeof(int));
    sorted_array = quick_sort(&selected_array);

    return 0;
}

int* quick_sort(int* selected_array) {
}