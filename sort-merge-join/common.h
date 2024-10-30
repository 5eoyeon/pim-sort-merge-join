#define INT64

#ifdef UINT64
#define T uint64_t
#elif defined(INT64)
#define T int64_t
#elif defined(DOUBLE)
#define T double
#endif

#define CACHE_SIZE 256

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
