#pragma once
#include "../application/wordCount.h"
#include "../dataframe/distributedDataframe.h"
#include <iostream>

void test() {
    WordCount wc = WordCount(0, "100k.txt", 1);
    wc.run_();
}

int main(int argc, char **argv) {
    test();
    return 0;
}