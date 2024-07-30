// dpu-upmem-dpurte-clang -DNR_TASKLETS=24 -o create_tasklet create_tasklet.c

#include <stdio.h>
#include <barrier.h>
#include <defs.h>
#include <stdint.h>

#define NR_TASKLETS 2

BARRIER_INIT(my_barrier, NR_TASKLETS);

int result[NR_TASKLETS];

int main() {
    uint32_t tasklet_id = me();

    result[tasklet_id] = tasklet_id;
    printf("Tasklet %d is running\n", tasklet_id);

    barrier_wait(&my_barrier);

    return 0;
}