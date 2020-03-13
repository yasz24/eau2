#pragma once
#include "fielder.h"
#include <iostream>
//authors: eldrid.s@husky.neu.edu and shetty.y@husky.neu.edu
class PrintFielder : public Fielder {
  public:
  void start(size_t r) {

  }

  void accept(bool b) {
    if(b) {
      std::cout<<"  <true>  ";
    }
    std::cout<<"  <false>  ";
  }

  void accept(float f) {
    std::cout<<"  <"<<f<<">  ";
  }

  void accept(int i) {
    std::cout<<"  <"<<i<<">  ";
  }

  void accept(String* s){
    std::cout<<"  <"<<s<<">  ";
  }

  void done() {
    std::cout<<"\n";
  }
};