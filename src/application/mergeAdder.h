#pragma once
#include "../dataframe/visitor.h"
#include "../utils/string.h"
#include "jvMap.h"

class MergeAdder : public Reader {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  MergeAdder(SIMap& map) : map_(map)  {}
 
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
    std::cout<<"map set word: "<<word->c_str()<<" and count "<<num->v<<"\n";
    return false;
  }
};