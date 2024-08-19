#define DEBUG
#define FILE_NAME "test_data(1).csv"

#define NR_DPUS 2
#define NR_TASKLETS 2

#define MAX_COL 10
#define MAX_ROW 100

#define JOIN_COL 2
#define JOIN_VAL 50

#define JOIN_KEY 0

typedef struct
{
    int col_num;
    int row_num;
} dpu_block_t;

typedef struct
{
    int dpu_id;
    int col_num;
    int row_num;
    int *arr;
} dpu_result_t;

typedef struct
{
    int tasklet_id;
    int row_num;
    int *arr;
} tasklet_result_t;