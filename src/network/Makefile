build:
	g++ server.cpp ../serialize/deserialize.cpp -o server -std=c++11 -w
	g++ client.cpp ../serialize/deserialize.cpp -o client -std=c++11 -w
run:
	#runs demo for m3. need to add sleeps though.
	./server -ip 127.0.0.1:3000 -nodes 3 &
	./client -ip 127.0.0.1:3001 -idx 1 &
	./client -ip 127.0.0.1:3002 -idx 2 &
clean:
	rm -rf ./server
	rm -rf ./client
