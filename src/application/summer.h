//lang::CwC
#pragma once
#include "../utils/string.h"
#include <stdio.h>
#include <stdlib.h>
#include "jvMap.h"

/** Uses visitor pattern to iterate through a map and return next string - int pair
 * to the provided row
 * authored by: vitekj@me.com
 */ 
class Summer : public Writer {
public:
  SIMap& map_;
  size_t i = 0;
  size_t j = 0;
  size_t seen = 0;
 
  Summer(SIMap& map) : map_(map) {}
 //Iterates to next expected map index
  void next() {
      if (i == map_.capacity_ ) return;
      if ( j < map_.items_[i].keys_.length() ) {
          j++;
          ++seen;
      } else {
          ++i;
          j = 0;
          while( i < map_.capacity_ && map_.items_[i].keys_.length() == 0 )  i++;
          //if (k()) ++seen;
      }
  }
 //checks if expected map location has contained data
  String* k() {
      if (i==map_.capacity_ || j == map_.items_[i].keys_.length()) {
        return nullptr;
        std::cout<<"we never get THIS nullptr\n";
      }
      return (String*) (map_.items_[i].keys_.get(j));
  }
 //gets val associated with string key at next index
  size_t v() {
      if (i == map_.capacity_ || j == map_.items_[i].keys_.length()) {
          assert(false); 
          return 0;
      }
      return ((Num*)(map_.items_[i].vals_.get(j)))->v;
  }
 //sets row string and int to next available entry in map
  void visit(Row& r) override {
      if (k() == nullptr) {
          next();
      }
      String & key = *k();
      size_t value = v();
      r.set(0, &key);
      r.set(1, (int) value);
      next();
  }
//returns true if every entry in the map has been accessed
  bool done() override {return seen == map_.size(); }
};