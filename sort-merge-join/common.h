#include <stdlib.h>

#define DEBUG
#define INT64

#define NR_DPUS 64
#define NR_TASKLETS 16

// #define SELECT_COL1 0
// #define SELECT_VAL1 5000

// #define SELECT_COL2 0
// #define SELECT_VAL2 5000

// #define JOIN_KEY1 0
// #define JOIN_KEY2 0

#define CACHE_SIZE 256

#ifdef UINT64
#define T uint64_t
#elif defined(INT64)
#define T int64_t
#elif defined(DOUBLE)
#define T double
#endif

typedef struct
{
    int table_num;
    int col_num;
    int row_num;
} dpu_block_t;

typedef struct
{
    int table_num;
    int dpu_id;
    int col_num;
    int row_num;
    T *arr;
} dpu_result_t;

typedef struct
{
    int tasklet_id;
    int row_num;
    T *arr;
} tasklet_result_t;

typedef struct Params
{
    char *input_file1;
    char *input_file2;
    int select_col1;
    int select_val1;
    int select_col2;
    int select_val2;
    int join_key1;
    int join_key2;
} Params;

Params p;

void input_params(int argc, char **argv)
{
    p.input_file1 = argv[1];
    p.input_file2 = argv[2];
    p.select_col1 = atoi(argv[3]);
    p.select_val1 = atoi(argv[4]);
    p.select_col2 = atoi(argv[5]);
    p.select_val2 = atoi(argv[6]);
    p.join_key1 = atoi(argv[7]);
    p.join_key2 = atoi(argv[8]);
}