#pragma once
#include "../utils/string.h"
#include <stdio.h>
#include <stdlib.h>
#include "jvMap.h"
class Summer : public Writer {
public:
  SIMap& map_;
  size_t i = 0;
  size_t j = 0;
  size_t seen = 0;
 
  Summer(SIMap& map) : map_(map) {}
 
  void next() {
      if (i == map_.capacity_ ) return;
      if ( j < map_.items_[i].keys_.length() ) {
          j++;
          ++seen;
      } else {
          ++i;
          j = 0;
          while( i < map_.capacity_ && map_.items_[i].keys_.length() == 0 )  i++;
          if (k()) ++seen;
      }
  }
 
  String* k() {
      if (i==map_.capacity_ || j == map_.items_[i].keys_.length()) {
        return nullptr;
      }
      return (String*) (map_.items_[i].keys_.get(j));
  }
 
  size_t v() {
      if (i == map_.capacity_ || j == map_.items_[i].keys_.length()) {
          assert(false); 
          return 0;
      }
      return ((Num*)(map_.items_[i].vals_.get(j)))->v;
  }
 
  void visit(Row& r) override {
      if (!k()) {
          next();
      }
      String & key = *k();
      size_t value = v();
      r.set(0, &key);
      r.set(1, (int) value);
      next();
  }
 
  bool done() override {return seen >= map_.size(); }
};