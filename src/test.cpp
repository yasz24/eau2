// Lang::CwC
//
#include <assert.h>
#include <stdio.h>

#include "parser.h"

char* cwc_strdup(const char* src) {
    char* result = new char[strlen(src) + 1];
    strcpy(result, src);
    return result;
}

void test_stringColumn() {
    StringColumn* col = new StringColumn();
    // Basic tests
    assert(col->getType() == ColumnType::STRING);
    col->append(cwc_strdup("test1"));
    col->appendMissing();
    col->append(cwc_strdup("test2"));
    assert(col->getLength() == 3);
    assert(col->isEntryPresent(0));
    assert(!col->isEntryPresent(1));
    assert(col->isEntryPresent(2));
    assert(strcmp(col->getEntry(0), "test1") == 0);
    assert(strcmp(col->getEntry(2), "test2") == 0);

    // Use StrColumn to run a large test to make sure arraylists expand correctly
    for (size_t i = 0; i < 1024; i++) {
        col->append(cwc_strdup("test1024"));
    }

    assert(col->getLength() == 1027);
    assert(strcmp(col->getEntry(100), "test1024") == 0);
    delete col;
}

void test_intColumn() {
    IntegerColumn* col = new IntegerColumn();
    // Basic int tests
    assert(col->getType() == ColumnType::INTEGER);
    col->appendMissing();
    col->appendMissing();
    col->append(1);
    col->append(2);
    assert(col->getLength() == 4);
    assert(col->isEntryPresent(3));
    assert(!col->isEntryPresent(0));
    assert(col->getEntry(3) == 2);
    delete col;
}

void test_floatColumn() {
    FloatColumn* col = new FloatColumn();
    // Basic float tests
    assert(col->getType() == ColumnType::FLOAT);
    col->append(4500.0);
    col->appendMissing();
    col->appendMissing();
    assert(col->getLength() == 3);
    assert(col->isEntryPresent(0));
    assert(!col->isEntryPresent(2));
    assert(col->getEntry(0) == 4500.0);
    delete col;
}

void test_boolColumn() {
    BoolColumn* col = new BoolColumn();
    // Basic bool tests
    assert(col->getType() == ColumnType::BOOL);
    col->append(true);
    col->append(false);
    col->appendMissing();
    assert(col->getLength() == 3);
    assert(!col->isEntryPresent(2));
    assert(col->isEntryPresent(1));
    assert(col->getEntry(1) == false);
    delete col;
}

void test_strSlice() {
    const char* test_str = "abcde   5.2  100 test";
    StrSlice slice1{test_str, 17, 21};
    assert(slice1.getLength() == 4);
    assert(slice1.getChar(0) == 't');
    assert(slice1.getChar(1) == 'e');

    StrSlice slice2{test_str, 5, 12};
    slice2.trim(' ');
    assert(slice2.getChar(0) == '5');
    assert(slice2.getChar(2) == '2');
    char* cstr = slice2.toCString();
    assert(strcmp(cstr, "5.2") == 0);
    delete[] cstr;
    assert(slice2.toFloat() == 5.2f);

    StrSlice slice3{test_str, 13, 16};
    assert(slice3.toInt() == 100);

    StrSlice slice4{"-128", 0, 4};
    assert(slice4.toInt() == -128);

    StrSlice slice5{"+437896", 0, 5};
    assert(slice5.toInt() == 4378);
}

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    printf("Running internal tests\n");
    test_stringColumn();
    test_intColumn();
    test_floatColumn();
    test_boolColumn();
    test_strSlice();
    printf("Success\n");
    return 0;
}
