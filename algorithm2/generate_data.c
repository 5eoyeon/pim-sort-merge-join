#include "join.h"

void create_directory(const char *dir_name)
{
    char command[100];
    sprintf(command, "mkdir -p %s", dir_name);
    system(command);
}

void remove_directory(const char *dir_name)
{
    char command[100];
    sprintf(command, "rm -rf %s", dir_name);
    system(command);
}

void generate_csv(const char *filename, int nrows)
{
    FILE *file = fopen(filename, "w");

    if (file == NULL)
    {
        printf("Error opening file %s\n", filename);
        return;
    }

    // header
    // fprintf(file, "id");
    // for (int i = 0; i < COLS - 1; i++)
    // {
    //     fprintf(file, ",col%d", i);
    // }
    // fprintf(file, "\n");

    for (int i = 0; i < nrows; i++)
    {
        int id = rand() % ID_MAX;
        fprintf(file, "%d", id);

        for (int j = 0; j < COLS - 1; j++)
        {
            fprintf(file, ",%d", rand() % 10000);
        }

        fprintf(file, "\n");
    }

    fclose(file);
}

void generate_data()
{
    remove_directory("table1");
    remove_directory("table2");

    create_directory("table1");
    create_directory("table2");

    // 랜덤
    // srand(time(NULL));

    for (int i = 0; i < FILENUM; i++)
    {
        char filename[100];
        sprintf(filename, "table1/%d.csv", i);
        generate_csv(filename, ROWS);

        sprintf(filename, "table2/%d.csv", i);
        generate_csv(filename, ROWS);
    }
}
