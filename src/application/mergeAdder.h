//lang::CwC
#pragma once
#include "../dataframe/visitor.h"
#include "../utils/string.h"
#include "jvMap.h"

/** Uses visitor pattern to add words from a dataframe to an existing map of words and counts
 * Heavily, heavily influenced (copied) from adder.h - authored by: vitekj@me.com
 * authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu
 */ 
class MergeAdder : public Reader {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  MergeAdder(SIMap& map) : map_(map)  {}
 //main difference between this and adder.h is if the word is not found in the map
 //MergeAdder sets the word value to the word's qty in df, adder sets to 1
  bool visit(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Num* num = nullptr;
    if(map_.contains(*word)) {
       num = map_.get(*word);
       assert(num != nullptr);
       num->v++;
    } else {
        num = new Num(r.get_int(1));
    }
    map_.set(*word, num);
    //std::cout<<"map set word: "<<word->c_str()<<" and count "<<num->v<<"\n";
    return false;
  }
};