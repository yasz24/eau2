build:
	cd tests; g++ -std=c++11 test_sorer_dataframe.cpp

run: 
	cd tests; ./a.out

valgrind:
	docker build -t memory-test:0.1 .
	docker run -ti -v `pwd`:/test memory-test:0.1 bash -c "cd /test/tests; g++ -o test test_sorer_dataframe.cpp && valgrind --leak-check=full ./test"

clean:
	cd ./tests/ ; rm a.out; rm test