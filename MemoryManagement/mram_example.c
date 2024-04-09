#include <mram.h>
#include <stdint.h>

#define BUFFER_SIZE 256

/* Buffer in MRAM. */
uint8_t __mram_noinit mram_array[BUFFER_SIZE];

int main() {
  /* A 256-bytes buffer in WRAM, containing the initial data. */
  __dma_aligned uint8_t input[BUFFER_SIZE];
  /* The other buffer in WRAM, where data are copied back. */
  __dma_aligned uint8_t output[BUFFER_SIZE];

  /* Populate the initial buffer. */
  for (int i = 0; i < BUFFER_SIZE; i++)
    input[i] = i;
  mram_write(input, mram_array, sizeof(input));

  /* Copy back the data. */
  mram_read(mram_array, output, sizeof(output));
  for (int i = 0; i < BUFFER_SIZE; i++)
    if (i != output[i])
      return 1;

  return 0;
}
