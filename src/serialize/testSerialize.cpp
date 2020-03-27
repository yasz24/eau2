#include <stdio.h>
#include <stdlib.h>

#include "../helper.h"
#include "deserialize.h"
#include "../store/value.h"
#include "../store/key.h"
#include "../store/kvstore.h"
#include "../utils/distributedArray.h"

//helper methods for testbuilding
KVStore* tempStore() {
    Value* v1 = new Value("muffin", 6);
    Value* v3 = new Value("carrot", 6);
    Key* k1 = new Key("payload_1", 0);
    Key* k3 = new Key("payload_3", 0);

    KVStore* kvs1 = new KVStore(2, 0);
    kvs1->put(k1, v1);
    kvs1->put(k3, v3);
    return kvs1;
}

void testValueSerialization() {
    Sys* system = new Sys();
    Value* v1 = new Value("muffin", 6);
    Value* v3 = new Value("carrot", 6);
    char* serialized = v1->serialize();
    std::cout<<serialized<<"\n";
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

void testDistributedArrays() {
    Sys* system = new Sys();
    KVStore* kvs1 = new KVStore(5, 0);
    IntDistributedArray* ida = new IntDistributedArray(kvs1, 15, 18);
    for(int i = 0; i < 10; i++) {
        ida->pushBack(i);
    }
    char* serialized = ida->serialize();
    IntDistributedArray* ida2 = new IntDistributedArray(serialized);
    String* s = new String(serialized);
    String* s2 = new String(ida2->serialize());
    system->t_true(s2->equals(s));
    system->OK("Passed intDistributedArray....");
}

int main() {
    testValueSerialization();
    //testKeySerialization();
    //testKVStoreSerialization(); //issue with map currently
    testDistributedArrays();
    return 0;
}