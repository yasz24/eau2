build:
	g++ server.cpp -o server -std=c++11
	g++ client.cpp -o client -std=c++11
run:
	./server -ip 127.0.0.1:8888 &
	./client -ip 127.0.0.1:50000 &
	./client -ip 127.0.0.1:50001 &
	./client -ip 127.0.0.1:50002 &
	./client -ip 127.0.0.1:50003 &
	./client -ip 127.0.0.1:50004 &
	./client -ip 127.0.0.1:50005 &
	./client -ip 127.0.0.1:50006 &
	./client -ip 127.0.0.1:50007 &
	./client -ip 127.0.0.1:50008 &
	./client -ip 127.0.0.1:50009 &
	# max clients hit, shutdown system.
	./client -ip 127.0.0.1:500010
clean:
	rm -rf ./server
	rm -rf ./client
