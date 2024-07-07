// gcc --std=c99 main.c -o main `dpu-pkg-config --cflags --libs dpu`

// 각 DPU에서 최대 24개의 tasklet 실행이 가능

// 랭크 당 1개의 DPU

// 프로그래머는... DPU 당 tasklet 수를 결정해야 함
// 64*24 -> 1*24

// 우리 구현에서는 24개로 쪼개고 배리어를 써서 다같이 만나게 하는 방법이...

// 그냥 랭크 여러개 쓰면 안됨? -> 써도 됨 근데 랭크 끼리 통신 못함 쓰는 의미 X

// 랭크 하나에 테이블 하나

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

  DPU_ASSERT(dpu_alloc(1, "backend=simulator", &set1)); // 1개의 DPU 할당, set1에 해당 DPU 세트를 저장
  DPU_ASSERT(dpu_load(set1, DPU_BINARY, NULL)); // set1에 할당된 DPU에 DPU_BINARY로 지정된 프로그램 로드
  DPU_ASSERT(dpu_launch(set1, DPU_SYNCHRONOUS)); // set1에 로드된 프로그램을 동기적으로 실행, DPU_SYNCHRONOUS로 프로그램이 완료될 때까지 기다림

  DPU_ASSERT(dpu_alloc(1, "backend=simulator", &set2));
  DPU_ASSERT(dpu_load(set2, DPU_BINARY, NULL));
  DPU_ASSERT(dpu_launch(set2, DPU_SYNCHRONOUS));

  DPU_FOREACH(set1, dpu1) { // set1에 포함된 모든 DPU에 대해 반복
    DPU_ASSERT(dpu_log_read(dpu1, stdout)); // 각 DPU의 로그를 읽어 출력
  }

  DPU_FOREACH(set2, dpu2) {
    DPU_ASSERT(dpu_log_read(dpu2, stdout));
  }

  DPU_ASSERT(dpu_free(set1)); // 자원 반환

  return 0;
}