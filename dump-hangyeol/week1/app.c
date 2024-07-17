#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef DPU_BINARY
#define DPU_BINARY "./task"
#endif

#define DPU_COUNT 2

int col_num = 0;
int row_num = 0;

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

int main(void)
{
    // alloc dpus
    struct dpu_set_t set, dpu;
    uint32_t dpu_id;
    DPU_ASSERT(dpu_alloc(DPU_COUNT, "backend=simulator", &set));
    DPU_ASSERT(dpu_load(set, DPU_BINARY, NULL));

    // set col_num, row_num
    set_csv_size("test_data.csv");
    int cut = row_num / DPU_COUNT;

    DPU_FOREACH(set, dpu, dpu_id)
    {
        uint64_t start = dpu_id * cut;
        uint64_t end = dpu_id * cut + cut;
        // DPU_ASSERT(dpu_copy_from(dpu, "start", 0, &start, sizeof(start)));
        // DPU_ASSERT(dpu_copy_from(dpu, "end", sizeof(start), &end, sizeof(start) + sizeof(end)));
    }

    DPU_ASSERT(dpu_launch(set, DPU_SYNCHRONOUS));

    DPU_FOREACH(set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }

    DPU_ASSERT(dpu_free(set));

    return 0;
}
