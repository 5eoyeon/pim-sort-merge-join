sort_merge_join: main.o sorts.o generate_data.o
	$(CC) $(CFLAGS) -o sort_merge_join main.o sorts.o generate_data.o
main.o: main.c
	$(CC) $(CFLAGS) -c main.c
sorts.o: sorts.c
	$(CC) $(CFLAGS) -c sorts.c
generate_data.o: generate_data.c
	$(CC) $(CFLAGS) -c generate_data.c
clean:
	rm -f *.o sort_merge_join