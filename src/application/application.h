//lang::CwC
#pragma once
#include "../object.h"
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
        this->kv_ = new KVStore(1, idx, network);
    }
    //Legacy single-node constructor. Currently used in WordCount app, hoping to phase out
    Application(size_t idx) {
        this->this_node_ = idx;
        this->kv_ = new KVStore(1, idx);
    }
    //Virtual run method that begins all Applications
    virtual void run_() {}

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