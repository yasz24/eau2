.PHONY: all test clean submit

PROG_NAME             = sorer
TEST_PROG_NAME        = a1-part1-test
HEADERS               = object.h column.h parser.h
SRC                   = main.cpp
TEST                  = test.cpp

CXXFLAGS = -std=gnu++11 -g -Wall -Wextra -O3

ifdef WITH_ASAN
CXX=clang++
CXXFLAGS += -fsanitize=address -lc++ -lrt
endif
ifdef WITH_MSAN
CXX=clang++
CXXFLAGS += -fsanitize=memory -lc++ -lrt
endif

all: $(TEST_PROG_NAME) $(PROG_NAME)

build: $(PROG_NAME)

$(PROG_NAME): $(SRC) $(HEADERS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<

$(TEST_PROG_NAME): $(TEST) $(HEADERS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -rdynamic -o $@ $< -ldl

clean:
	$(RM) $(TEST_PROG_NAME) $(PROG_NAME)

test: $(TEST_PROG_NAME) $(PROG_NAME)
	./$(TEST_PROG_NAME)
	# Basics tests
	./test_sor.sh "BOOL" -f "data.sor" -from 0 -len 100 -print_col_type 0
	./test_sor.sh "STRING" -f "data.sor" -from 0 -len 100 -print_col_type 2
	./test_sor.sh "0" -f "data.sor" -from 0 -len 100 -is_missing_idx 2 0
	./test_sor.sh "1" -f "data.sor" -from 0 -len 100 -is_missing_idx 2 1
	./test_sor.sh "1" -f "data.sor" -from 0 -len 100 -is_missing_idx 2 2
	./test_sor.sh '"hi"' -f "data.sor" -from 0 -len 100 -print_col_idx 2 0
	./test_sor.sh "12" -f "data.sor" -from 5 -len 100 -print_col_idx 1 0
	./test_sor.sh '2.200000' -f "data1.sor" -print_col_idx 2 0
	# Error conditions
	./test_sor.sh 'No such column' -f "data1.sor" -print_col_idx 6 0
	./test_sor.sh 'No such entry' -f "data1.sor" -print_col_idx 1 999
	# More complicated data
	./test_sor.sh '1' -f "data1.sor" -is_missing_idx 1 1
	./test_sor.sh "100" -f "data2.sor" -print_col_idx 2 1
	# Check that first line gets cut off with -from
	./test_sor.sh "2" -f "data2.sor" -from 5 -print_col_idx 2 1
	./test_sor.sh "2" -f "data2.sor" -from 5 -print_col_idx 2 1
	./test_sor.sh "3" -f "data2.sor" -from 5 -print_col_idx 2 2
	# Check last line cut off with -len
	./test_sor.sh "No such entry" -f "data2.sor" -from 5 -len 60 -print_col_idx 2 2
	# String parsing
	./test_sor.sh "4" -f "data3.sor" -print_col_idx 3 0
	./test_sor.sh '"string with > inside "' -f "data3.sor" -print_col_idx 2 0

docker-stage:
	docker build -t cs4500a1 .
	docker run -it -v $(PWD):/data cs4500a1 /bin/bash -c "cd /data; make test"
