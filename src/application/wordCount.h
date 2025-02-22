//lang::CwC
#pragma once
#include "application.h"
#include "../dataframe/distributedDataframe.h"
#include "../network/network_ip.h"
#include "../utils/keyBuff.h"
#include "fileReader.h"
#include "adder.h"
#include "summer.h"
#include "mapPrinter.h"
#include "mergeAdder.h"
#include "jvMap.h"
/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
/** c.e & y.s: 
 * Adapated from initial project briefing by engineering team to better fit current implementation
 * Will be adjusted in the next sprint to better reflect existing technologies.
 * Currently reliant on Corporate provided map implementation that adds extra tech debt
 * and does not fully support the distributed infrastructure we're trying to bring in across the board
 * will be updated with each week
 */
class WordCount: public Application {
public:
  static const size_t BUFSIZE = 1024;
  Key in;
  KeyBuff* kbuf;
  SIMap all;
  char* fileName_;
  size_t num_nodes_;
 
  WordCount(size_t idx, NetworkIP* network, char* fileName, size_t num_nodes):
    Application(idx, network), in("data", idx), kbuf{ new KeyBuff(new Key("wc-map-",0))}, fileName_{ fileName }, num_nodes_{num_nodes} {}
 
  /** The master nodes reads the input, then all of the nodes count. */
  void run_() override {
    if (kv_->this_node_ == 0) {
      //FileReader fr;
      FileReader fr(fileName_);
      delete DistributedDataFrame::fromVisitor(&in, kv_, "S", &fr);
    }
    local_count();
    reduce();
  }
 
  /** Returns a key for given node.  These keys are homed on master node
   *  which then joins them one by one. */
  Key* mk_key(size_t idx) {
      kbuf->c(idx);
      Key * k = kbuf->get();
      std::cout << "Created key :" << k->name()->c_str()<<"\n";
      return k;
  }
 
  /** Compute word counts on the local node and build a data frame. */
  void local_count() {
    //Value* temp = kv_->waitAndget(&in);
    Value* temp = kv_->get(&in);
    DistributedDataFrame* words = new DistributedDataFrame(temp->data, kv_);
    p("Node ").p(kv_->this_node_).pln(": starting local count...");
    SIMap map;
    Adder add(map);
    words->local_map(add);
    
    delete words;
    Summer cnt(map);
    Key* k = mk_key(kv_->this_node_);
    std::cout<<"size of map: "<< map.size() << "\n";
    //delete DistributedDataFrame::fromVisitor(mk_key(index), kv_, "SI", cnt);
    delete DistributedDataFrame::fromVisitor(k, kv_, "SI", &cnt);
  }
 
  /** Merge the data frames of all nodes */
  void reduce() {
    if (kv_->this_node_ != 0) return;
    pln("Node 0: reducing counts...\n");
    SIMap map;
    Key* own = mk_key(0);
    DistributedDataFrame* ddf = new DistributedDataFrame(kv_->get(own)->data, kv_);
    merge(ddf, map);
    for (size_t i = 1; i < num_nodes_; ++i) { // merge other nodes
      Key* ok = mk_key(i);
      //DistributedDataFrame* temp_ddf = new DistributedDataFrame(kv_->waitAndget(ok)->data, kv_);
      DistributedDataFrame* temp_ddf = new DistributedDataFrame(kv_->get(ok)->data, kv_);
      merge(temp_ddf, map);
      delete ok;
    }
    p("Different words: ").pln(map.size());
    //here we go...
    MapPrinter mp(map);
    while(!mp.done()) {
      mp.getNextVal();
    }
    delete own;
  }
 
  void merge(DistributedDataFrame* df, SIMap& m) {
    MergeAdder add(m);
    df->map(add);
    delete df;
  }
}; // WordcountDemo