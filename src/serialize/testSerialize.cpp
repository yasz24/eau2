#include <stdio.h>
#include <stdlib.h>

#include "../helper.h"
#include "deserialize.h"
#include "../store/value.h"
#include "../store/key.h"
#include "../store/kvstore.h"

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
    Key* v1 = new Key("muffin", 6);
    Key* v3 = new Key("carrot", 6);
    char* serialized = v1->serialize();
    Key* v2 = new Key(serialized);
    system->t_true(v1->equals(v2));
    system->t_false(v3->equals(v2));
    system->OK("Passed Key serialization");
    delete [] system;
};

void testKVStoreSerialization() {
    Sys* system = new Sys();
    Value* v1 = new Value("muffin", 6);
    Value* v3 = new Value("carrot", 6);
    Key* k1 = new Key("payload_1", 1);
    Key* k3 = new Key("payload_3", 1);

    KVStore* kvs1 = new KVStore(2, 1);
    kvs1->put(k1, v1);
    kvs1->put(k3, v3);
    char* serialized = kvs1->serialize();
    std::cout<<serialized<<"\n";
    KVStore* kvs2 = new KVStore(serialized);
    char* serialized2 = kvs2->serialize();
    std::cout<<serialized2<<"\n";
    String* s = new String(serialized);
    String* s2 = new String(serialized2);
    system->t_true(s2->equals(s));
    //system->t_true(kvs2->get(k1)->equals(v1));
    system->OK("Passed KVStore serialization");
}

void testDistributedArrays() {

}

int main() {
    testValueSerialization();
    testKeySerialization();
    testKVStoreSerialization(); //issue with map currently
    testDistributedArrays();
    return 0;
}