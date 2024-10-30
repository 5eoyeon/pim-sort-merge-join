#define DEBUG
#define INT64

#define NR_DPUS 64
#define NR_TASKLETS 16

#define SELECT_COL1 0
#define SELECT_VAL1 5000

#define SELECT_COL2 0
#define SELECT_VAL2 5000

#define JOIN_KEY1 0
#define JOIN_KEY2 0

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
