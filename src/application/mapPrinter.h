//lang::C++
#pragma once
#include "../utils/string.h"
#include <stdio.h>
#include <stdlib.h>
#include "jvMap.h"

/** Uses visitor pattern as a template for printing out an entire map
 * Heavily, heavily influenced (copied) from summer.h - authored by: vitekj@me.com
 * authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu
 */ 
class MapPrinter {
public:
  SIMap& map_;
  size_t i = 0;
  size_t j = 0;
  size_t seen = 0;
 
  MapPrinter(SIMap& map) : map_(map) {}

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
 //checks if the next expected map location has stored data
  String* k() {
      if (i==map_.capacity_ || j == map_.items_[i].keys_.length()) {
        return nullptr;
      }
      return (String*) (map_.items_[i].keys_.get(j));
  }
 //gets data at next expected map location
  size_t v() {
      if (i == map_.capacity_ || j == map_.items_[i].keys_.length()) {
          assert(false); 
          return 0;
      }
      return ((Num*)(map_.items_[i].vals_.get(j)))->v;
  }
 //prints the next string - number pairing in the map and advances the iterator
  void getNextVal() {
      if (k() == nullptr) {
          next();
      }
      String & key = *k();
      size_t value = v();
      std::cout<<key.c_str()<<" : "<<value<<"\n";
      next();
  }
 //checks if all map entries have been seen already
  bool done() {return seen == map_.size(); }
};