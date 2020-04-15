//lang::CwC
#pragma once
#include "../src/application/wordCount.h"
#include "../src/network/network_ip.h"
#include "../src/dataframe/distributedDataframe.h"
#include <iostream>

//authors: eldrid.s@husky.neu.edu shetty.y@husky.neu.edu
/*very simple test that runs the 100k.txt file proving it works effectively
* previous smaller scale testing has included variations of the "test.txt"
* since we're printing to console this included making documents with a set
* number of each word and checking for output. The test has now been set up
* to show of the 100k in a single node environment
*/
void test() {
    NetworkIP network;
    WordCount wc = WordCount(0, &network,"100k.txt", 1);
    wc.run_();
}

int main(int argc, char **argv) {
    test();
    return 0;
}