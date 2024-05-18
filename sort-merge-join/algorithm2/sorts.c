#include "join.h"

// 2열 기준

void selection_sort(FILE *file, FILE *temp_file)
{
    int data[ROWS][COLS];
    for (int i = 0; i < ROWS; i++)
    {
        fscanf(file, "%d,%d", &data[i][0], &data[i][1]);
    }

    for (int i = 0; i < ROWS - 1; i++)
    {
        int min = i;
        for (int j = i + 1; j < ROWS; j++)
        {
            if (data[j][0] < data[min][0])
            {
                min = j;
            }
        }

        int temp = data[i][0];
        data[i][0] = data[min][0];
        data[min][0] = temp;

        temp = data[i][1];
        data[i][1] = data[min][1];
        data[min][1] = temp;
    }

    for (int i = 0; i < ROWS; i++)
    {
        fprintf(temp_file, "%d,%d\n", data[i][0], data[i][1]);
    }

    rewind(temp_file);
}

void bubble_sort(FILE *file, FILE *temp_file)
{
    int data[ROWS][COLS];
    for (int i = 0; i < ROWS; i++)
    {
        fscanf(file, "%d,%d", &data[i][0], &data[i][1]);
    }

    for (int i = 0; i < ROWS - 1; i++)
    {
        for (int j = 0; j < ROWS - i - 1; j++)
        {
            if (data[j][0] > data[j + 1][0])
            {
                int temp = data[j][0];
                data[j][0] = data[j + 1][0];
                data[j + 1][0] = temp;

                temp = data[j][1];
                data[j][1] = data[j + 1][1];
                data[j + 1][1] = temp;
            }
        }
    }

    for (int i = 0; i < ROWS; i++)
    {
        fprintf(temp_file, "%d,%d\n", data[i][0], data[i][1]);
    }

    rewind(temp_file);
}

void insertion_sort(FILE *file, FILE *temp_file)
{
    int data[ROWS][COLS];
    for (int i = 0; i < ROWS; i++)
    {
        fscanf(file, "%d,%d", &data[i][0], &data[i][1]);
    }

    for (int i = 1; i < ROWS; i++)
    {
        int key = data[i][0];
        int key2 = data[i][1];
        int j = i - 1;

        while (j >= 0 && data[j][0] > key)
        {
            data[j + 1][0] = data[j][0];
            data[j + 1][1] = data[j][1];
            j = j - 1;
        }
        data[j + 1][0] = key;
        data[j + 1][1] = key2;
    }

    for (int i = 0; i < ROWS; i++)
    {
        fprintf(temp_file, "%d,%d\n", data[i][0], data[i][1]);
    }

    rewind(temp_file);
}

void merge_sort(FILE *file, FILE *temp_file)
{
}

void quick_sort(FILE *file, FILE *temp_file)
{
}