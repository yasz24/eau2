build:
	cd tests; g++ -w -std=c++11 testSerialize.cpp ../src/serialize/deserialize.cpp -g
	cd src/network; g++ server.cpp ../serialize/deserialize.cpp -o eau2-server -std=c++11 -w
	cd src/network; g++ client.cpp ../serialize/deserialize.cpp -o eau2-client -std=c++11 -w

run: 
	cd tests; ./a.out

demo:
	#runs demo for m3 - producer,consumer,summarizer
	cd src/network; ./eau2-server -ip 127.0.0.1:3000 -nodes 3 &
	sleep 2
	cd src/network; ./eau2-client -ip 127.0.0.1:3001 -idx 1 &
	sleep 2
	cd src/network; ./eau2-client -ip 127.0.0.1:3002 -idx 2 &
	sleep 10
	killall -9 eau2-server
	killall -9 eau2-client

valgrind:
	docker build -t memory-test:0.1 .
	docker run -ti -v `pwd`:/test memory-test:0.1 bash -c "cd /test/tests; g++ -o test test_sorer_dataframe.cpp && valgrind --leak-check=full ./test"

clean:
	cd tests ; rm a.out; rm -rf a.out.dSYM;
	cd src/network; rm eau2-server; rm eau2-client