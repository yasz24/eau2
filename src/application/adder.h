//lang::CwC
#pragma once
#include "../dataframe/visitor.h"
#include "../utils/string.h"
#include "jvMap.h"

/** Functional Adder for DF instances to count all instances of each word
 * in the first column.
 *  author: vitekj@me.com */
class Adder : public Reader {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  Adder(SIMap& map) : map_(map)  {}
 
  //Given a row, updates the stored map with word in the first column
  //i.e. if it's new, add to map. If not, increase count of word in map
  bool visit(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Num* num = map_.contains(*word) ? map_.get(*word) : new Num();
    assert(num != nullptr);
    num->v++;
    map_.set(*word, num);
    return false;
  }
};