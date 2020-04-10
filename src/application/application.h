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
        this_node_ = idx;
        kv_ = new KVStore(network->num_nodes_, idx, network);
    }
    //Legacy single-node constructor. Currently used in WordCount app, hoping to phase out
    Application(size_t idx) {
        this_node_ = idx;
        kv_ = new KVStore(1, idx);
    }

    void start_kv() {
      kv_->start();
    }

    //Virtual run method that begins all Applications
    virtual void run_() {}

    size_t this_node() {
      return this_node_;
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
 
  Demo(size_t idx, NetworkIP* network): Application(idx, network) {}
 
  void run_() override {
    std::cout << "in demo run\n";
    std::cout << this_node() << "\n";
    switch(this_node()) {
    case 0:   producer();     break;
    case 1:   counter();      break;
    case 2:   summarizer();
   }
  }
 
  void producer() {
    std::cout << "producer\n";
    size_t SZ = 2*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    DistributedDataFrame::fromArray(main, kv_, SZ, vals);
    DistributedDataFrame::fromScalar(check, kv_, sum);
  }
 
  void counter() {
    std::cout << "counter\n";
    Deserializable ds;
    Value* val = kv_->waitAndget(main);
    DistributedDataFrame* v = dynamic_cast<DistributedDataFrame*>(ds.deserialize(val->data, kv_));
    size_t sum = 0;
    for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
    p("The sum is  ").pln(sum);
    DistributedDataFrame::fromScalar(verify, kv_, (int)sum);
  }
 
  void summarizer() {
    std::cout << "summarizer\n";
    Deserializable ds;
    Value* verify_val = kv_->waitAndget(verify);
    Value* check_val = kv_->waitAndget(check);
    DistributedDataFrame* result = dynamic_cast<DistributedDataFrame*>(ds.deserialize(verify_val->data, kv_));
    DistributedDataFrame* expected = dynamic_cast<DistributedDataFrame*>(ds.deserialize(check_val->data, kv_));
    pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
  }
};