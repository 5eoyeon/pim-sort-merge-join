#include <handshake.h>
#include <stdint.h>
#include <defs.h>

#define TASKLET_1 1

uint32_t message;

static uint32_t complex_operation_0(uint32_t x) { return x + 1; }

static uint32_t complex_operation_1(uint32_t x) { return x * 3; }

static uint32_t complex_operation_2(uint32_t x, uint32_t y) { return y - x; }

uint32_t task2(uint32_t result0, uint32_t result1) {
  return complex_operation_2(result0, result1);
}

int task1() {
  uint32_t result;
  uint32_t param = 0x19;

  result = complex_operation_1(param);

  message = result;
  handshake_notify();

  return 0;
}

int task0() {
  uint32_t result;
  uint32_t param = 0x42;

  result = complex_operation_0(param);

  handshake_wait_for(TASKLET_1);
  result = task2(result, message);

  return result;
}

int (*tasks[])(void) = {task0, task1};
int main() {
  return tasks[me()]();
}
