#include <barrier.h>
#include <defs.h>
#include <stdint.h>

BARRIER_INIT(my_barrier, NR_TASKLETS);

uint8_t coefficients[128];

/* Computes the sum of coefficients within a tasklet specific range. */
int compute_checksum() {
  int i, checksum = 0;
  for (i = 0; i < 32; i++)
    checksum += coefficients[(me() << 5) + i];
  return checksum;
}

/* Initializes the coefficient table. */
void setup_coefficients() {
  int i;
  for (i = 0; i < 128; i++) {
    coefficients[i] = i;
  }
}

/* The main thread initializes the table and joins the barrier to wake up the
 * other tasklets. */
int master() {
  setup_coefficients();
  barrier_wait(&my_barrier);
  int result = compute_checksum();
  return result;
}

/* Tasklets wait for the initialization to complete, then compute their
 * checksum. */
int slave() {
  barrier_wait(&my_barrier);
  int result = compute_checksum();
  return result;
}

int main() {
  return me() == 0 ? master(): slave();
}
