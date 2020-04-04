#include "../object.h"
#include "../store/kvstore.h"
#include "../dataframe/distributedDataframe.h"
#include "../serialize/deserialize.h"

class Application : public Object {
public:
    KVStore* kv_;
    size_t this_node_;

    Application(size_t idx, NetworkIP* network) {
        this->this_node_ = idx;
        this->kv_ = new KVStore(1, idx, network);
    }

};

class Trivial : public Application {
 public:
  Trivial(size_t idx, NetworkIP* network) : Application(idx, network) { }
  void run_() {
    size_t SZ = 1000*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    //std::cout << sum << "\n";
    Key key("triv",0);
    DistributedDataFrame* df = DistributedDataFrame::fromArray(&key, kv_, SZ, vals);
    // std::cout << "here\n";
    //std::cout << df->serialize() << "\n";
    assert(df->get_double(0,1) == 1);
    Deserializable* ds = new Deserializable();
    DistributedDataFrame* df2 = dynamic_cast<DistributedDataFrame*>(ds->deserialize(kv_->get(&key)->data, kv_));
    //std::cout << df2->serialize() << "\n";
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
    //std::cout << sum << "\n";
    assert(sum==0);
    delete df; delete df2;
  }
};


// class Demo : public Application {
// public:
//   Key main("main",0);
//   Key verify("verif",0);
//   Key check("ck",0);
 
//   Demo(size_t idx): Application(idx) {}
 
//   void run_() override {
//     switch(this_node()) {
//     case 0:   producer();     break;
//     case 1:   counter();      break;
//     case 2:   summarizer();
//    }
//   }
 
//   void producer() {
//     size_t SZ = 100*1000;
//     double* vals = new double[SZ];
//     double sum = 0;
//     for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
//     DataFrame::fromArray(&main, &kv, SZ, vals);
//     DataFrame::fromScalar(&check, &kv, sum);
//   }
 
//   void counter() {
//     DataFrame* v = kv.waitAndGet(main);
//     size_t sum = 0;
//     for (size_t i = 0; i < 100*1000; ++i) sum += v->get_double(0,i);
//     p("The sum is  ").pln(sum);
//     DataFrame::fromScalar(&verify, &kv, sum);
//   }
 
//   void summarizer() {
//     DataFrame* result = kv.waitAndGet(verify);
//     DataFrame* expected = kv.waitAndGet(check);
//     pln(expected->get_double(0,0)==result->get_double(0,0) ? "SUCCESS":"FAILURE");
//   }
// };


// // Application {
// // NetworkIP
// // KVStore

// //  void listen() {
// //         while (true) {
// //         Message* m =  recv_msg();
// //         //process
// //             //put
            
// //             //get
// //       }
// //   }

// // }