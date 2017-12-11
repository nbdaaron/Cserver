all:
	gcc sorter_server.c -O3 -o sorter_server -lpthread
	gcc sorter_client.c -O3 -o sorter_client -lpthread

clean:
	rm sorter_server
	rm sorter_client

debug:
	gcc sorter_server.c -Wall -Werror -fsanitize=address -g -o sorter_server -lpthread
	gcc sorter_client.c -Wall -Werror -fsanitize=address -g -o sorter_client -lpthread

gdbdebug:
	gcc sorter_server.c -Wall -fsanitize=address -g -o sorter_server -lpthread
	gcc sorter_client.c -Wall -fsanitize=address -g -o sorter_client -lpthread

gdbs:
	gdb --args ./sorter_server -p 3030

gdbc:
	gdb --args ./sorter_client -c director_name -h man.cs.rutgers.edu -p 3030 -d t -o output

runs:
	./sorter_server -p 3030

runc:
	./sorter_client -c "director_name" -h man.cs.rutgers.edu -p 3030 -d t -o output