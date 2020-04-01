#include "../object.h"
#include "../store/kvstore.h"
#include "../dataframe/distributedDataframe.h"
#include "../serialize/deserialize.h"

class Application : public Object {
public:
    KVStore* kv_;
    size_t this_node_;

    Application(size_t idx) {
        this->this_node_ = idx;
        this->kv_ = new KVStore(1, idx);
    }

};

class Trivial : public Application {
 public:
  Trivial(size_t idx) : Application(idx) { }
  void run_() {
    size_t SZ = 2*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    //std::cout << sum << "\n";
    Key key("triv",0);
    DistributedDataFrame* df = DistributedDataFrame::fromArray(&key, kv_, SZ, vals);
    //std::cout << df->serialize() << "\n";
    assert(df->get_double(0,1) == 1);
    Deserializable* ds = new Deserializable();
    DistributedDataFrame* df2 = dynamic_cast<DistributedDataFrame*>(ds->deserialize(kv_->get(&key)->data, kv_));
    //std::cout << df2->serialize() << "\n";
    //std::cout << "here\n";
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
    //std::cout << sum << "\n";
    assert(sum==0);
    delete df; delete df2;
  }
};