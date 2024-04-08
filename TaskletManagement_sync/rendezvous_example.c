#include <defs.h>
#include <sem.h>

SEMAPHORE_INIT(my_semaphore, 0);

int factorial_result = 0;

int factorial10() {
  int i = 10, f = 1;
  for (; i > 0; i--) {
    f = i * f;
  }
  return f;
}

int producer() {
  factorial_result = factorial10();
  sem_give(&my_semaphore);
  sem_give(&my_semaphore);

  int result_producer = (me() << 24) | factorial_result;
  return result_producer;
}

int consumer() {
  sem_take(&my_semaphore);

  int result_consumer = (me() << 24) | factorial_result;
  return result_consumer;
}

int (*tasks[])(void) = {producer, consumer, consumer};
int main() { return tasks[me()](); }
