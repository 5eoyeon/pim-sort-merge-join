// gcc --std=c99 main.c -o main `dpu-pkg-config --cflags --libs dpu`

#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>

#ifndef DPU_BINARY
#define DPU_BINARY "./create_tasklet"
#endif

int main(void) {
  struct dpu_set_t set1, dpu1;
  struct dpu_set_t set2, dpu2;

  DPU_ASSERT(dpu_alloc(64, "backend=simulator", &set1));
  DPU_ASSERT(dpu_load(set1, DPU_BINARY, NULL));
  DPU_ASSERT(dpu_launch(set1, DPU_SYNCHRONOUS));

  DPU_ASSERT(dpu_alloc(1, "backend=simulator", &set2));
  DPU_ASSERT(dpu_load(set2, DPU_BINARY, NULL));
  DPU_ASSERT(dpu_launch(set2, DPU_SYNCHRONOUS));

  DPU_FOREACH(set1, dpu1) {
    DPU_ASSERT(dpu_log_read(dpu1, stdout));
  }

  DPU_FOREACH(set2, dpu2) {
    DPU_ASSERT(dpu_log_read(dpu2, stdout));
  }

  DPU_ASSERT(dpu_free(set1));

  return 0;
}
