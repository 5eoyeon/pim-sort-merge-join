#include <alloc.h>
#include <stdlib.h>
#include <string.h>

// List of CSV data, processed by the program
// A list of cities, along with their states.
static const char *city_list[] = {"New York,New York", "Los Angeles,California",
                                  "Chicago,Illinois",  "Houston,Texas",
                                  "Phoenix,Arizona",   NULL};

static char *get_state_name_from_csv(const char *csv) {
  char *state_name = strchr(csv, ',') + 1;
  int result_len = strlen(state_name) + 2;
  char *result = buddy_alloc(result_len);
  (void)strcpy(result, state_name);
  result[result_len - 2] = ',';
  result[result_len - 1] = '\0';
  return result;
}

static char *add_state_to_all_states(char *all_states, char *new_state) {
  int all_states_len = strlen(all_states);
  int new_len = strlen(new_state) + all_states_len;
  char *new_all_states = buddy_alloc(new_len);
  strcpy(new_all_states, all_states);
  strcpy(&new_all_states[all_states_len], new_state);

  buddy_free(all_states);
  buddy_free(new_state);
  return new_all_states;
}

int main() {
  int i;
  char *all_states;
  buddy_init(4096);

  all_states = buddy_alloc(1);
  *all_states = '\0';

  for (i = 0; city_list[i] != NULL; i++) {
    char *state_name = get_state_name_from_csv(city_list[i]);
    all_states = add_state_to_all_states(all_states, state_name);
  }

  return 0;
}
