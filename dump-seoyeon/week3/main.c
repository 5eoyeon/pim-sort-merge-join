// gcc --std=c99 main.c -o main `dpu-pkg-config --cflags --libs dpu`

#include <assert.h>
#include <dpu.h>
#include <dpu_log.h>
#include <stdio.h>

#define NR_DPUS 5
#define NR_TASKLETS 2
#define SIZE 100

#ifndef DPU_BINARY
#define DPU_BINARY "./lock"
#endif

int main(void) {
  struct dpu_set_t set1, dpu1;
  uint32_t dpu_id;

  int test_array[SIZE];
  for(int i = 0; i < SIZE; i++) test_array[i] = i;

  DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &set1)); // 1개의 DPU 할당, set1에 해당 DPU 세트를 저장
  DPU_ASSERT(dpu_load(set1, DPU_BINARY, NULL)); // set1에 할당된 DPU에 DPU_BINARY로 지정된 프로그램 로드
  
  // DPU 내에서 나눠갖기
  int chunk_size = SIZE / NR_DPUS;
  DPU_FOREACH(set1, dpu1, dpu_id) {
    int offset = dpu_id * chunk_size;

    DPU_ASSERT(dpu_prepare_xfer(dpu1, test_array + offset));
    DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_TO_DPU, "test_array", 0, chunk_size * sizeof(int), DPU_XFER_DEFAULT));
  }
  
  DPU_ASSERT(dpu_launch(set1, DPU_SYNCHRONOUS)); // set1에 로드된 프로그램을 동기적으로 실행, DPU_SYNCHRONOUS로 프로그램이 완료될 때까지 기다림

  // Retrieve
  DPU_FOREACH(set1, dpu1, dpu_id) {
    int offset = dpu_id * chunk_size;

    DPU_ASSERT(dpu_prepare_xfer(dpu1, test_array + offset));
    DPU_ASSERT(dpu_push_xfer(dpu1, DPU_XFER_FROM_DPU, "test_array", 0, chunk_size * sizeof(int), DPU_XFER_DEFAULT));
  }

  DPU_FOREACH(set1, dpu1) { // set1에 포함된 모든 DPU에 대해 반복
    DPU_ASSERT(dpu_log_read(dpu1, stdout)); // 각 DPU의 로그를 읽어 출력
  }
  
  printf("\nresult: ");
  for (int i = 0; i < SIZE; i++) printf("%d ", test_array[i]);

  DPU_ASSERT(dpu_free(set1)); // 자원 반환

  return 0;
}