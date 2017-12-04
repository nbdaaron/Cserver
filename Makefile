all:
	gcc Sorter.c -O3 -o sorter -lpthread

clean:
	rm sorter

debug:
	gcc Sorter.c -Wall -Werror -fsanitize=address -g -o sorter -lpthread

gdbdebug:
	gcc Sorter.c -Wall -fsanitize=address -g -o sorter -lpthread

gdb:
	gdb --args ./sorter -p 8000

run:
	./sorter -p 8000
