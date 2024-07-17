#include <stdio.h>
#include <defs.h>
#include <mram.h>

#define TASKLET_COUNT 2

uint64_t __mram_noinit start = 0;
uint64_t __mram_noinit end = 0;

int main()
{
    printf("tasklet %u: start = %lu, end = %lu\n", me(), start, end);
    return 0;
}
