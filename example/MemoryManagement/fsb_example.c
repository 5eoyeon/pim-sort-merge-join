#include <alloc.h>
#include <stddef.h>

#define BLOCK_SIZE (sizeof(list_t))
#define NB_OF_BLOCKS (1000)

typedef struct _list_t list_t;

struct _list_t {
  int data;
  list_t *next;
};

fsb_allocator_t allocator;

static void initialize_allocator() {
  allocator = fsb_alloc(BLOCK_SIZE, NB_OF_BLOCKS);
}

static list_t *add_head_data(list_t *list, int data) {
  list_t *new_data = fsb_get(allocator);

  if (new_data == NULL)
    return NULL;

  new_data->data = data;
  new_data->next = list;

  return new_data;
}

static list_t *populate_list() {
  list_t *list = NULL;

  list = add_head_data(list, 42);
  list = add_head_data(list, 1);
  list = add_head_data(list, -2);
  list = add_head_data(list, 13);
  list = add_head_data(list, 22);
  list = add_head_data(list, 10000);
  list = add_head_data(list, 0);
  list = add_head_data(list, 91);
  list = add_head_data(list, -45);
  list = add_head_data(list, 9);
  list = add_head_data(list, 0);

  return list;
}

static void clean_list(list_t *list) {
  list_t *current = list;

  while (current != NULL) {
    list_t *tmp = current;
    current = current->next;
    fsb_free(allocator, tmp);
  }
}

int main() {
  list_t *list;
  int result = 0;

  /* Allocator initialization */
  initialize_allocator();

  /* List initialization with some data */
  list = populate_list();

  list_t *current = list;
  list_t *previous = NULL;

  /* Filtering out data that is even */
  while (current != NULL) {
    if ((current->data % 2) == 0) {
      list_t *tmp = current;
      current = current->next;
      fsb_free(allocator, tmp);

      if (previous != NULL) {
        previous->next = current;
      } else {
        list = current;
      }
    } else {
      previous = current;
      current = current->next;
    }
  }

  current = list;

  /* Computing the sum of the data in the list */
  while (current != NULL) {
    result += current->data;
    current = current->next;
  }

  /* Cleaning the list */
  clean_list(list);

  return result;
}
