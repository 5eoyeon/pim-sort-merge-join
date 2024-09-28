#define DEBUG
#define INT64
#define FILE_NAME_1 "test_data(1).csv"
#define FILE_NAME_2 "test_data(2).csv"

#define NR_DPUS 5
#define NR_TASKLETS 2

// #define MAX_COL 10
// #define MAX_ROW 200

#define SELECT_COL 2
#define SELECT_VAL 50

#define JOIN_KEY1 0
#define JOIN_KEY2 1

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
