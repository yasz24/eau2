//lang::CwC
#pragma once
#include "../object.h"
#include "../store/key.h"
#include "../store/kvstore.h"
#include "../dataframe/distributedDataframe.h"
#include "../serialize/deserialize.h"

/** Basic interpretation of a DF Application and an example trivial use-case
 * authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu
 */ 
class Application : public Object {
public:
    KVStore* kv_;
    size_t this_node_;

    //Networked Application Constructor
    Application(size_t idx, NetworkIP* network) {
        this->this_node_ = idx;
        this->kv_ = new KVStore(network->num_nodes_, idx, network);
    }
    //Legacy single-node constructor. Currently used in WordCount app, hoping to phase out
    Application(size_t idx) {
        this->this_node_ = idx;
        this->kv_ = new KVStore(1, idx);
    }
    //Virtual run method that begins all Applications
    virtual void run_() {}

    size_t this_node() {
      kv_->num_nodes_;
    }
};

//Trivial use case of the application class using networking
class Trivial : public Application {
 public:
  Trivial(size_t idx, NetworkIP* network) : Application(idx, network) { }
  void run_() {
    size_t SZ = 1000*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key key("triv",0);
    DistributedDataFrame* df = DistributedDataFrame::fromArray(&key, kv_, SZ, vals);
    assert(df->get_double(0,1) == 1);
    Deserializable* ds = new Deserializable();
    DistributedDataFrame* df2 = dynamic_cast<DistributedDataFrame*>(ds->deserialize(kv_->get(&key)->data, kv_));
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
    assert(sum==0);
    delete df; delete df2;
  }
};


class Demo : public Application {
public:
  Key* main =  new Key("main", 0);
  Key* verify = new Key("verif",0);
  Key* check = new Key("ck",0);
 
  Demo(size_t idx): Application(idx) {}
 
  void run_() override {
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    size_t SZ = 100*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DistributedDataFrame::fromArray(main, kv_, SZ, vals);
    DataFrame::fromScalar(&check, &kv, sum);
  }
 
  void counter() {
    DistributedDataFrame* v = dynamic_cast<DistributedDataFrame*>(kv_->waitAndget(main));
    size_t sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum);
    DataFrame::fromScalar(&verify, kv_, sum);
  }
 
  void summarizer() {
    DistributedDataFrame* result = dynamic_cast<DistributedDataFrame*>(kv_->waitAndget(verify));
    DistributedDataFrame* expected = dynamic_cast<DistributedDataFrame*>(kv_->waitAndget(check));
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
  }
};