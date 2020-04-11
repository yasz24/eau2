// Lang::CwC
//adapted from main.cpp supplied by euhlmann for Sorer writing
//used by Chev Eldrid and Yash Shetty
//https://github.ccs.neu.edu/euhlmann/CS4500-A1-part1
#include <stdio.h>
#include <stdlib.h>

#include "../src/sorer/parser.h"
#include "../src/helper.h"
#include "../src/dataframe/summation.h"
/**
 * Enum representing different states of parsing command line arguments.
 */
enum class ParseState {
    DEFAULT,
    FLAG_F,
    FLAG_FROM,
    FLAG_LEN,
    FLAG_COL_TYPE,
    FLAG_COL_IDX_COL,
    FLAG_COL_IDX_OFF,
    FLAG_MISSING_IDX_COL,
    FLAG_MISSING_IDX_OFF
};

/**
 * Helper function to terminate with an error print if the given bool is not true.
 * @param test The bool
 */
void cli_assert(bool test) {
    if (!test) {
        printf("Unexpected command line arguments provided\n");
        exit(-1);
    }
}
/**
 * Asserts that the given ssize_t has not already been changed from -1, and then
 * parses the given c-style string as long to set it.
 * @param arg_loc The location of the ssize_t to work with
 * @param arg A string containing a long to parse
 */
void parse_size_t_arg(ssize_t* arg_loc, char* arg) {
    cli_assert(*arg_loc == -1);
    *arg_loc = atol(arg);
}
/**
 * Parses command line args given by argc and argv. Updates the given ssize_t pointers to
 * -1 for each arg that is not present on the command line (nullptr for file), or the value of that
 * command line argument.
 * @param argc, argv The command line arguments
 * @param file Pointer to result of parsing -f
 * @param start Pointer to result of parsing -from
 * @param len Pointer to result of parsing -len
 * @param col_type Pointer to result of parsing -print_col_type
 * @param col_idx_col, col_idx_off Pointer to result of parsing -print_col_idx
 * @param missing_idx_col, missing_idx_off Pointer to result of parsing -is_missing_idx
 */

/**
 * The main function.
 */
void testDataFrame() {
    Sys* system = new Sys();
    // Parse arguments
    char* filename = "../data/data.sor";
    // -1 represents argument not provided
    ssize_t start = -1;
    ssize_t len = -1;
    ssize_t col_type = -1;
    ssize_t col_idx_col = -1;
    ssize_t col_idx_off = -1;
    ssize_t missing_idx_col = -1;
    ssize_t missing_idx_off = -1;

    // Open requested file
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
        printf("Failed to open file\n");
        return exit(1);
    }
    fseek(file, 0, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    // Set argument defaults
    if (start == -1) {
        start = 0;
    }

    if (len == -1) {
        len = file_size - start;
    }

    {
        // Run parsing
        SorParser parser{file, (size_t)start, (size_t)start + len, file_size};
        parser.guessSchema();
        parser.parseFile();
        DataFrame* df = parser.getDataFrame();
        system->t_true(df->get_int(1,1) == 12);
        system->t_true(df->get_string(2,0)->equals(new String("hi")));
    }

    fclose(file);
    system->OK("Passed Dataframe creation test");
}

void testDistributedDataFrame() {
    Sys* system = new Sys();
    Key uK("usrs");
    KVStore* kv_ = new KVStore(1, 0);
    // Parse arguments
    const char* filename = "../data/data.sor";
    DistributedDataFrame* df = DistributedDataFrame::fromFile(filename, &uK, kv_);
    system->t_true(df->get_int(1,1) == 12);
    system->t_true(df->get_string(2,0)->equals(new String("hi")));
}

void testSummationRower() {
    Sys* system = new Sys();
    // Parse arguments
    char* filename = "../data/data4.sor";
    // -1 represents argument not provided
    ssize_t start = -1;
    ssize_t len = -1;
    ssize_t col_type = -1;
    ssize_t col_idx_col = -1;
    ssize_t col_idx_off = -1;
    ssize_t missing_idx_col = -1;
    ssize_t missing_idx_off = -1;

    // Open requested file
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
        printf("Failed to open file\n");
        return exit(1);
    }
    fseek(file, 0, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    // Set argument defaults
    if (start == -1) {
        start = 0;
    }

    if (len == -1) {
        len = file_size - start;
    }

    {
        // Run parsing
        SorParser parser{file, (size_t)start, (size_t)start + len, file_size};
        parser.guessSchema();
        parser.parseFile();
        DataFrame* df = parser.getDataFrame();
        Summation* sum = new Summation(df);
        df->map(*sum);
        system->t_true(sum->sum_ == 2000);
    }

    fclose(file);
    system->OK("Passed Summation rower test");
}

int main() {
    testDataFrame();
    testDistributedDataFrame();
    testSummationRower();
    return 0;
}
