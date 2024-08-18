#define DEBUG
#define FILE_NAME "test_data(1).csv"

#define NR_DPUS 3
#define NR_TASKLETS 3

#define MAX_COL 10
#define MAX_ROW 100

#define JOIN_COL 2
#define JOIN_VAL 50

#define JOIN_KEY 0

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