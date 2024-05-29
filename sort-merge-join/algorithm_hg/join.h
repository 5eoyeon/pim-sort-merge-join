#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define ROWS 10000
#define COLS 2
#define FILENUM 1
#define ID_MAX 10000

// generate data
void generate_data();

// sort
void selection_sort(FILE *file, FILE *temp_file);
void bubble_sort(FILE *file, FILE *temp_file);
void insertion_sort(FILE *file, FILE *temp_file);
void merge_sort(FILE *file, FILE *temp_file);
void quick_sort(FILE *file, FILE *temp_file);