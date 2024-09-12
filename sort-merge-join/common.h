#define DEBUG
#define FILE_NAME "test_data(2).csv"

#define NR_DPUS 2
#define NR_TASKLETS 4

#define MAX_COL 10
#define MAX_ROW 1000

#define SELECT_COL 2
#define SELECT_VAL 500

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

#define divceil(n, m) (((n) - 1) / (m) + 1)