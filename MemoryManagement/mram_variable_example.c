#include <alloc.h>
#include <mram.h>
#include <stdint.h>

/* Buffer in MRAM. */
uint32_t __mram_noinit mram_array[4];

uint32_t input[4] = {0, 2, 4, 6};
uint32_t output[4];

int main() {
  for (int i = 0; i < 4; ++i) {
    mram_array[i] = input[i];
  }

  for (int i = 0; i < 4; ++i) {
    output[i] = mram_array[i];
  }

  return 0;
}
