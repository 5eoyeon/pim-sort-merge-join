#define DEBUG
#define FILE_NAME "test_data.csv"

#define NR_DPUS 2
#define NR_TASKLETS 2

#define MAX_COL 10
#define MAX_ROW 10

#define JOIN_COL 2
#define JOIN_VAL 5

#define JOIN_KEY 0

typedef struct
{
    int col_num;
    int row_num;
} dpu_arguments;

typedef struct
{
    int dpu_id;
    int row_num;
    int arr[MAX_ROW * MAX_COL];
} dpu_result;

typedef struct
{
    int tasklet_id;
    int row_num;
    int *arr;
} tasklet_result;