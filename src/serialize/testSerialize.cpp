#include <stdio.h>
#include <stdlib.h>

#include "../helper.h"
#include "deserialize.h"
#include "../store/value.h"
#include "../store/key.h"
#include "../store/kvstore.h"
#include "../utils/distributedArray.h"
#include "../dataframe/distributedColumn.h"
#include "../dataframe/distributedDataframe.h"
#include "../dataframe/distributedRow.h"

//helper methods for testbuilding
KVStore* tempStore() {
    Value* v1 = new Value("muffin", 6);
    Value* v3 = new Value("carrot", 6);
    Key* k1 = new Key("payload_1", 0);
    Key* k3 = new Key("payload_3", 0);

    KVStore* kvs1 = new KVStore(1, 0);
    kvs1->put(k1, v1);
    kvs1->put(k3, v3);
    return kvs1;
}

StringArray* genStringArray() {
    StringArray* sa = new StringArray();
    sa->pushBack(new String("taco"));
    sa->pushBack(new String("hamburger"));
    sa->pushBack(new String("ice cream"));
    sa->pushBack(new String("pizza"));
    sa->pushBack(new String("french fry"));
    sa->pushBack(new String("lasagna"));
    sa->pushBack(new String("curry"));
    sa->pushBack(new String("lo mein"));
    sa->pushBack(new String("pad thai"));
    sa->pushBack(new String("chicken"));
    return sa;
}

void testValueSerialization() {
    Sys* system = new Sys();
    Value* v1 = new Value("muffin", 6);
    Value* v3 = new Value("carrot", 6);
    char* serialized = v1->serialize();
    Value* v2 = new Value(serialized);
    system->t_true(v1->equals(v2));
    system->t_false(v3->equals(v2));
    system->OK("Passed value serialization");
    delete [] system;
}

void testKeySerialization() {
    Sys* system = new Sys();
    Key* v1 = new Key("muffin", 0);
    Key* v3 = new Key("carrot", 0);
    char* serialized = v1->serialize();
    Key* v2 = new Key(serialized);
    system->t_true(v1->equals(v2));
    system->t_false(v3->equals(v2));
    system->OK("Passed Key serialization");
    delete [] system;
};

void testKVStoreSerialization() {
    Sys* system = new Sys();
    KVStore* kvs1 = tempStore();
    char* serialized = kvs1->serialize();
    KVStore* kvs2 = new KVStore(serialized);
    char* serialized2 = kvs2->serialize();
    String* s = new String(serialized);
    String* s2 = new String(serialized2);
    system->t_true(s2->equals(s));
    //system->t_true(kvs2->get(k1)->equals(v1));
    system->OK("Passed KVStore serialization");
    delete kvs1;
    delete [] system;
}

void testIntDistributedArrays() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    IntDistributedArray* ida = new IntDistributedArray(kvs1);
    for(int i = 0; i < 10; i++) {
        //std::cout << "here\n";
        ida->pushBack(i);
    }
    char* serialized = ida->serialize();
    IntDistributedArray* ida2 = new IntDistributedArray(serialized);
    //std::cout << "here\n";
    //system->t_true(ida->equals(ida2)); //problem child
    String* s = new String(serialized);
    String* s2 = new String(ida2->serialize());
    system->t_true(s2->equals(s));
    system->OK("Passed intDistributedArray");
    delete [] system;
}

void testStringDistributedArrays() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    StringDistributedArray* sda = new StringDistributedArray(kvs1);
    StringArray* sa = genStringArray();
    for(int i = 0; i < 10; i++) {
        sda->pushBack(sa->get(i));
    }
    char* serialized = sda->serialize();
    StringDistributedArray* sda2 = new StringDistributedArray(serialized);
    String* s = new String(serialized);
    String* s2 = new String(sda2->serialize());
    system->t_true(s2->equals(s));
    for(int i = 0; i < 10; i++) {
        system->t_true(sda->get(i)->equals(sa->get(i)));
    }
    system->OK("Passed StringDistributedArray");
    delete sa;
    delete [] system;
}

void testFloatDistributedArrays() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    FloatDistributedArray* fda = new FloatDistributedArray(kvs1);
    for(int i = 0; i < 40; i++) {
        fda->pushBack(i);
    }
    char* serialized = fda->serialize();   
    FloatDistributedArray* fda2 = new FloatDistributedArray(serialized);
    String* s = new String(serialized);
    char* serial = fda2->serialize();
    String* s2 = new String(fda2->serialize());
    system->t_true(s2->equals(s));
    for(int i = 0; i < 10; i++) {
        system->t_true(fda->get(i) == i);
    }
    system->OK("Passed FloatDistributedArray");
    delete [] system;
}

void testDistributedIntColumn() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    DistributedIntColumn* dfc = new DistributedIntColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    std::cout << serialized << "\n";   
    DistributedIntColumn* fda2 = new DistributedIntColumn(serialized);
    String* s = new String(serialized);
    char* serial = fda2->serialize();
    String* s2 = new String(fda2->serialize());
    system->t_true(s2->equals(s));
    //std::cout << "here\n";
    // for(int i = 0; i < 10; i++) {
    //     system->t_true(dfc->get(i) == i);
    // }
    system->OK("Passed DistributedIntColumn");
    delete [] system;
}

void testDistributedDoubleColumn() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    DistributedDoubleColumn* dfc = new DistributedDoubleColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedDoubleColumn* fda2 = new DistributedDoubleColumn(serialized);
    String* s = new String(serialized);
    char* serial = fda2->serialize();
    String* s2 = new String(fda2->serialize());
    system->t_true(s2->equals(s));
    //std::cout << "here\n";
    // for(int i = 0; i < 10; i++) {
    //     system->t_true(dfc->get(i) == i);
    // }
    system->OK("Passed DistributedDoubleColumn");
    delete [] system;
}

void testDistributedFloatColumn() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    DistributedFloatColumn* dfc = new DistributedFloatColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedFloatColumn* fda2 = new DistributedFloatColumn(serialized);
    String* s = new String(serialized);
    char* serial = fda2->serialize();
    String* s2 = new String(fda2->serialize());
    system->t_true(s2->equals(s));
    //std::cout << "here\n";
    // for(int i = 0; i < 10; i++) {
    //     system->t_true(dfc->get(i) == i);
    // }
    system->OK("Passed DistributedFloatColumn");
    delete [] system;
}

void testDistributedBoolColumn() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    DistributedBoolColumn* dfc = new DistributedBoolColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedBoolColumn* fda2 = new DistributedBoolColumn(serialized);
    String* s = new String(serialized);
    char* serial = fda2->serialize();
    String* s2 = new String(fda2->serialize());
    system->t_true(s2->equals(s));
    //std::cout << "here\n";
    // for(int i = 0; i < 10; i++) {
    //     system->t_true(dfc->get(i) == i);
    // }
    system->OK("Passed DistributedBoolColumn");
    delete [] system;
}

void testDistributedStringColumn() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(1, 0);
    DistributedStringColumn* dfc = new DistributedStringColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedStringColumn* fda2 = new DistributedStringColumn(serialized);
    String* s = new String(serialized);
    char* serial = fda2->serialize();
    String* s2 = new String(fda2->serialize());
    system->t_true(s2->equals(s));
    //std::cout << "here\n";
    // for(int i = 0; i < 10; i++) {
    //     system->t_true(dfc->get(i) == i);
    // }
    system->OK("Passed DistributedStringColumn");
    delete [] system;
}

void testSchema() {
    Sys* system = new Sys();
    Schema* sch = new Schema("IFD");
    char* serialized = sch->serialize();
    //std::cout << serialized << "\n";
    Schema* sch2 = new Schema(serialized);
    //std::cout << sch2->serialize() << "\n";
    String* s = new String(serialized);
    String* s2 = new String(sch2->serialize());
    system->t_true(s2->equals(s));

    system->OK("Passed Schema");
    delete [] system;
}

void testDistributedDataframe() {
    Sys* system = new Sys();
    Schema* sch = new Schema();
    KVStore* kvs1 = new KVStore(1, 0);
    DistributedIntColumn* ic = new DistributedIntColumn(kvs1);
    for (int i = 0; i < 50; i++) {
        sch->add_row(nullptr);
        ic->push_back(i);
    }

    DistributedDoubleColumn* dc = new DistributedDoubleColumn(kvs1);
    for (double i = 0; i < 50; i++) {
        dc->push_back(i);
    }
    DistributedDataFrame* dd = new DistributedDataFrame(*sch, kvs1);
    dd->add_column(ic, nullptr);
    dd->add_column(dc, nullptr);

    char* serialized = dd->serialize();
    std::cout << serialized << "\n";   
    DistributedDataFrame* dd2 = new  DistributedDataFrame(serialized);
    String* s = new String(serialized);
    std::cout << dd2->serialize() << "\n";
    String* s2 = new String(dd2->serialize());
    system->t_true(s2->equals(s));

    system->OK("Passed DistributedDataFrame");
    delete [] system;
}

int main() {
    // testValueSerialization();
    // testKeySerialization();
    // testKVStoreSerialization(); //issue with map currently
    // testIntDistributedArrays();
    // testStringDistributedArrays();
    // testFloatDistributedArrays();
    //testDistributedIntColumn();
    // testDistributedDoubleColumn();
    // testDistributedFloatColumn();
    // testDistributedBoolColumn();
    // testDistributedStringColumn();
    //testSchema();
    testDistributedDataframe();
    return 0;
}