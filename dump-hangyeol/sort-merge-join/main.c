#include "join.h"

typedef struct
{
    int id;
    int col;
} Data;

int read_data(FILE *file, Data *data)
{
    return fscanf(file, "%d,%d\n", &data->id, &data->col) == 2;
}

void join(FILE *file1, FILE *file2, FILE *output)
{
    Data data1;
    Data data2;

    Data *buffer1 = malloc(sizeof(Data));
    Data *buffer2 = malloc(sizeof(Data));

    while (1)
    {
        if (!read_data(file1, buffer1) || !read_data(file2, buffer2))
        {
            break;
        }

        if (buffer1->id == buffer2->id)
        {

            fprintf(output, "%d,%d,%d\n", buffer1->id, buffer1->col, buffer2->col);
        }
        else if (buffer1->id < buffer2->id)
        {
            while (buffer1->id < buffer2->id)
            {
                if (!read_data(file1, buffer1))
                {
                    break;
                }
            }
        }
        else
        {
            while (buffer1->id > buffer2->id)
            {
                if (!read_data(file2, buffer2))
                {
                    break;
                }
            }
        }
    }
}

void sort_merge_join(FILE *file1, FILE *file2, FILE *output)
{
    // file1을 id 기준으로 정렬
    char temp1[] = "temp1.csv";
    FILE *temp1_file = fopen(temp1, "w+");
    if (temp1_file == NULL)
    {
        printf("Error opening file\n");
        return;
    }
    insertion_sort(file1, temp1_file);

    // file2를 id 기준으로 정렬
    char temp2[] = "temp2.csv";
    FILE *temp2_file = fopen(temp2, "w+");
    if (temp2_file == NULL)
    {
        printf("Error opening file\n");
        return;
    }
    insertion_sort(file2, temp2_file);

    // file1과 file2를 join하여 output에 저장
    join(temp1_file, temp2_file, output);

    // 임시 파일 삭제
    fclose(temp1_file);
    fclose(temp2_file);
    remove(temp1);
    remove(temp2);
}

int main()
{
    // generate_data();

    clock_t start, end;
    double result;

    FILE *file1 = fopen("table1/0.csv", "r");
    FILE *file2 = fopen("table2/0.csv", "r");
    FILE *output = fopen("output.csv", "w");

    if (file1 == NULL || file2 == NULL || output == NULL)
    {
        printf("Error opening files\n");
        return 1;
    }

    start = clock();
    sort_merge_join(file1, file2, output);
    end = clock();

    result = (double)(end - start) / CLOCKS_PER_SEC;
    printf("Time: %f\n", result);

    fclose(file1);
    fclose(file2);
    fclose(output);

    return 0;
}