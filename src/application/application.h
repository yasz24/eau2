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
    size_t SZ = 1000*1000;
    int* vals = new int[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key key("triv",0);
    DistributedDataFrame* df = DistributedDataFrame::fromArray(&key, kv_, SZ, vals);
    assert(df->get_int(0,1) == 1);
    Deserializable* ds = new Deserializable();
    DistributedDataFrame* df2 = dynamic_cast<DistributedDataFrame*>(ds->deserialize(kv_->get(&key)->data));
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
    assert(sum==0);
    delete df; delete df2;
  }
};