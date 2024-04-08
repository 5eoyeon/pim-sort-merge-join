#include <defs.h>
#include <mram.h>
#include <mutex_pool.h>

#define BUFFER_SIZE (1024 * 1024)
#define NR_ELEMENTS_HIST (1 << 8)
#define NR_ELEMENTS_PER_TASKLET (BUFFER_SIZE / NR_TASKLETS)

__mram_noinit uint8_t input_table[BUFFER_SIZE];
__mram uint64_t histogram[NR_ELEMENTS_HIST];

/**
 * Create a mutex pool of size 8 to protect access to the histogram.
 **/
MUTEX_POOL_INIT(my_mutex_pool, 8);

int main() {

  for (unsigned i = me() * NR_ELEMENTS_PER_TASKLET;
       i < (me() + 1) * NR_ELEMENTS_PER_TASKLET; ++i) {
    uint8_t elem = input_table[i];
    /**
     * Lock the element with id 'elem'.
     * The call to mutex_pool_lock is internally locking the
     * hardware mutex of id 'elem & 7'.
     **/
    mutex_pool_lock(&my_mutex_pool, elem);
    histogram[elem]++;
    mutex_pool_unlock(&my_mutex_pool, elem);
  }
}
