// gcc --std=c99 main.c -o main `dpu-pkg-config --cflags --libs dpu`

#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>

#define NR_DPUS 10
#define NR_TASKLETS 10
#define SIZE 5000

#ifndef DPU_BINARY
#define DPU_BINARY "./lock"
#endif

int main(void) {
  struct dpu_set_t set1, dpu1;
  uint32_t dpu_id;

  int test_array[SIZE];
  for(int i = 0; i < SIZE; i++) test_array[i] = i;

  DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set1));
  DPU_ASSERT(dpu_load(set1, DPU_BINARY, NULL));
  
  // assign in each DPU
  int chunk_size = SIZE / NR_DPUS;
  DPU_FOREACH(set1, dpu1, dpu_id) {
    int offset = dpu_id * chunk_size;

    DPU_ASSERT(dpu_prepare_xfer(dpu1, test_array + offset));
    DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_TO_DPU, "test_array", 0, chunk_size * sizeof(int), DPU_XFER_DEFAULT));
  }
  
  DPU_ASSERT(dpu_launch(set1, DPU_SYNCHRONOUS)); // execute synchronously

  // Retrieve
  DPU_FOREACH(set1, dpu1, dpu_id) {
    int offset = dpu_id * chunk_size;

    DPU_ASSERT(dpu_prepare_xfer(dpu1, test_array + offset));
    DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_FROM_DPU, "test_array", 0, chunk_size * sizeof(int), DPU_XFER_DEFAULT));
  }

  DPU_FOREACH(set1, dpu1) {
    DPU_ASSERT(dpu_log_read(dpu1, stdout));
  }
  
  printf("\nresult: ");
  for (int i = 0; i < SIZE; i++) printf("%d ", test_array[i]);
  printf("\n");

  DPU_ASSERT(dpu_free(set1));

  return 0;
}