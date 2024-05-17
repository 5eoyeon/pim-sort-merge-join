#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int id;
    char name[25];
    int age;
} table1data;

typedef struct {
    int id;
    int salary;
    char dept[25];
} table2data;

typedef struct {
    int id;
    char name[25];
    int age;
    int salary;
    char dept[25];
} data;

void select_table1(const char *filename, table1data db[], int *idx) {
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Error opening file");
        exit(0);
    }

    char line[1024];
    fgets(line, sizeof(line), file);

    *idx = 0;
    while (fgets(line, sizeof(line), file)) {
        table1data cur;
        if(sscanf(line, "%d,%24[^,],%d", &cur.id, cur.name, &cur.age) == 3) {
            if(cur.age >= 40) {
                db[*idx] = cur;
                (*idx)++;
            }
        }
    }

    fclose(file);
}

void select_table2(const char *filename, table2data db[], int *idx) {
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Error opening file");
        exit(0);
    }

    char line[1024];
    fgets(line, sizeof(line), file);

    *idx = 0;
    while (fgets(line, sizeof(line), file)) {
        table2data cur;
        if(sscanf(line, "%d,%d,%24[^\n]", &cur.id, &cur.salary, cur.dept) == 3) {
            if(cur.salary > 45000) {
                db[*idx] = cur;
                (*idx)++;
            }
        }
    }

    fclose(file);
}

void sort_table1(table1data db[], int num) {
    int minIndex;
    table1data cur;
    for (int i = 0; i < num-1; i++) {
        minIndex = i;
        for (int j = i + 1; j < num; j++) {
            if (db[j].id < db[minIndex].id) {
                minIndex = j;
            }
        }

        cur = db[i];
        db[i] = db[minIndex];
        db[minIndex] = cur;
    }
}

void sort_table2(table2data db[], int num) {
    int minIndex;
    table2data cur;
    for (int i = 0; i < num-1; i++) {
        minIndex = i;
        for (int j = i + 1; j < num; j++) {
            if (db[j].id < db[minIndex].id) {
                minIndex = j;
            }
        }

        cur = db[i];
        db[i] = db[minIndex];
        db[minIndex] = cur;
    }
}

void join(table1data table1[], int num_table1, table2data table2[], int num_table2, data joined[], int *num_joined) {
    int i = 0, j = 0;
    *num_joined = 0;

    while (i < num_table1 && j < num_table2) {
        if (table1[i].id == table2[j].id) {
            joined[*num_joined].id = table1[i].id;
            strcpy(joined[*num_joined].name, table1[i].name);
            joined[*num_joined].age = table1[i].age;
            joined[*num_joined].salary = table2[j].salary;
            strcpy(joined[*num_joined].dept, table2[j].dept);
            (*num_joined)++;
            i++;
            j++;
        } else if (table1[i].id < table2[j].id) {
            i++;
        } else {
            j++;
        }
    }
}

int main() {
    table1data table1_db[100];
    table2data table2_db[100];
    data result_db[100];
    int num_table1 = 0;
    int num_table2 = 0;
    int num_result = 0;

    // input & select
    // sort
    select_table1("table1.csv", table1_db, &num_table1);
    sort_table1(table1_db, num_table1);

    select_table2("table2.csv", table2_db, &num_table2);
    sort_table2(table2_db, num_table2);
    
    // join
    join(table1_db, num_table1, table2_db, num_table2, result_db, &num_result);

    // output
    FILE *outfile = fopen("joined_table.csv", "w");
    if (outfile == NULL) {
        perror("Error creating file");
        exit(1);
    }

    fprintf(outfile, "ID,Name,Age,Salary,Dept\n");

    // Write data
    for (int i = 0; i < num_result; i++) {
        fprintf(outfile, "%d,%s,%d,%d,%s\n",
                result_db[i].id, result_db[i].name, result_db[i].age,
                result_db[i].salary, result_db[i].dept);
    }

    fclose(outfile);

    return 0;
}