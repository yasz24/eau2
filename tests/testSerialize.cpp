#include <stdio.h>
#include <stdlib.h>

#include "../src/helper.h"
#include "../src/serialize/deserialize.h"
#include "../src/store/value.h"
#include "../src/store/key.h"
#include "../src/store/kvstore.h"
#include "../src/utils/distributedArray.h"
#include "../src/dataframe/distributedColumn.h"
#include "../src/dataframe/distributedDataframe.h"
#include "../src/dataframe/distributedRow.h"
#include "../src/application/application.h"
#include "../src/network/network.h"
#include <map>

//helper methods for testbuilding
KVStore* tempStore() {
    Value* v1 = new Value("muffin", 6);
    Value* v3 = new Value("carrot", 6);
    Key* k1 = new Key("payload_1", 0);
    Key* k3 = new Key("payload_3", 0);

    NetworkIP network;

    KVStore* kvs1 = new KVStore(1, 0, &network);
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


/**
 * Tests StringArray serialization and deserialization by comparing the serialization of a StringArray
 * to the serialized representation of a StringArray built by deserializing the initial array
 * 
 * Also tests the serialization of the intitial array to an expected output
 */ 
void testStringArraySerialization() {
    Sys* tester = new Sys();
    Deserializable* ds = new Deserializable();
    StringArray* is = new StringArray();
    is->pushBack(new String("taco"));
    is->pushBack(new String("dorito"));
    is->pushBack(new String("nacho"));
    is->pushBack(new String("enchilada"));
    char* string_is_serialized = is->serialize();
    StringArray* is_two = dynamic_cast<StringArray*>(ds->deserialize(string_is_serialized));
    tester->t_true(strcmp(string_is_serialized, is_two->serialize()) == 0);
    delete is;
    tester->OK("Passed String Array Serialization Tests");
    delete tester;
    delete [] ds;
}

void testStringColumnSerialization() {
    Sys* tester = new Sys();
    Deserializable* ds = new Deserializable();
    StringColumn* sc = new StringColumn();
    sc->push_back(new String("taco"));
    sc->push_back(new String("dorito"));
    sc->push_back(new String("nacho"));
    sc->push_back(new String("enchilada"));
    char* string_is_serialized = sc->serialize();
    StringColumn* is_two = dynamic_cast<StringColumn*>(ds->deserialize(string_is_serialized));
    tester->t_true(strcmp(string_is_serialized, is_two->serialize()) == 0);
    delete sc;
    tester->OK("Passed String Column Serialization Tests");
    delete tester;
    delete [] ds;
}

/**
 * Tests FloatArray serialization and deserialization by comparing the serialization of a FloatArray
 * to the serialized representation of a FloatArray built by deserializing the initial array
 * 
 */ 
void testFloatArraySerialization() {
    Sys* tester = new Sys();
    Deserializable* ds = new Deserializable();
    FloatArray* fa = new FloatArray(60);
    for(int i = 0; i < 50; i++) {
        fa->pushBack(i);
    }
    char* float_is_serialized = fa->serialize();
    FloatArray* fa_two = dynamic_cast<FloatArray*>(ds->deserialize(float_is_serialized));
    tester->t_true(strcmp(float_is_serialized, fa_two->serialize()) == 0);
    delete fa;
    tester->OK("Passed Float Array Serialization Tests");
    delete tester;
    delete [] ds;
}

void testFloatColumnSerialization() {
    Deserializable* ds = new Deserializable();
    Sys* tester = new Sys();
    FloatColumn* fa = new FloatColumn(60);
    for(int i = 0; i < 50; i++) {
        fa->push_back(i);
    }
    char* float_is_serialized = fa->serialize();
    FloatColumn* fa_two = dynamic_cast<FloatColumn*>(ds->deserialize(float_is_serialized));
    tester->t_true(strcmp(float_is_serialized, fa_two->serialize()) == 0);
    delete fa;
    tester->OK("Passed Float Array Serialization Tests");
    delete tester;
    delete [] ds;
}

/**
 * Tests IntArray serialization and deserialization by comparing the serialization of a IntArray
 * to the serialized representation of a IntArray built by deserializing the initial array
 * 
 */ 
void testIntArraySerialization() {
    Sys* tester = new Sys();
    Deserializable* ds = new Deserializable();
    IntArray* ia = new IntArray(100);
    for(int i = 0; i < 100; i++) {
        ia->pushBack(i);
    }
    char* int_is_serialized = ia->serialize();
    IntArray* ia_two = dynamic_cast<IntArray*>(ds->deserialize(int_is_serialized));
    tester->t_true(strcmp(int_is_serialized, ia_two->serialize()) == 0);
    delete ia;
    tester->OK("Passed Int Array Serialization Tests");
    delete tester;
    delete [] ds;
}

void testIntColumnSerialization() {
    Sys* tester = new Sys();
    Deserializable* ds = new Deserializable();
    IntColumn* ia = new IntColumn(120);
    for(int i = 0; i < 100; i++) {
        ia->push_back(i);
    }
    char* int_is_serialized = ia->serialize();
    IntColumn* ia_two = dynamic_cast<IntColumn*>(ds->deserialize(int_is_serialized));
    tester->t_true(strcmp(int_is_serialized, ia_two->serialize()) == 0);
    delete ia;
    tester->OK("Passed Int Column Serialization Tests");
    delete tester;
    delete [] ds;
}

/**
 * Tests Message serialization and deserialization by comparing the serialization of a Message
 * to the serialized representation of a Message built by deserializing the initial message
 * 
 * Also compares serialized message to expected JSON value
 * 
 */ 
void testMessageSerialization() {
    Sys* tester = new Sys();
    Message* msg = new Message(1, 2, 3, 5);
    char* msg_serialized = msg->serialize();
    Message* msg_two = new Message(msg_serialized);
    char* expected = "{ 'Message' : { 'kind_' : '1', 'sender_' : '2', 'target_' : '3', 'id_' : '5',  } }";
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->t_true(strcmp(msg_serialized, expected) == 0);
    tester->OK("Passed Message Serialization Tests");
    delete tester;
    delete msg;
}

void testAckSerialization() {
    Sys* tester = new Sys();
    Ack ack(1,2,3);
    char* msg_serialized = ack.serialize();
    Ack* msg_two = new Ack(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Ack Serialization Tests");
    delete tester;
    delete msg_two;
}


void testNackSerialization() {
    Sys* tester = new Sys();
    Nack ack(1,2,3);
    char* msg_serialized = ack.serialize();
    Nack* msg_two = new Nack(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Nack Serialization Tests");
    delete tester;
    delete msg_two;
}

void testPutSerialization() {
    Sys* tester = new Sys();
    Key k("key1", 0);
    Value v("val", 3);
    Put ack(&k, &v, 1,2,3);
    char* msg_serialized = ack.serialize();
    Put* msg_two = new Put(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Put Serialization Tests");
    delete tester;
    delete msg_two;
}

void testGetSerialization() {
    Sys* tester = new Sys();
    Key k("key1", 0);
    Get ack(&k, 1,2,3);
    char* msg_serialized = ack.serialize();
    Get* msg_two = new Get(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Get Serialization Tests");
    delete tester;
    delete msg_two;
}

void testWaitAndGetSerialization() {
    Sys* tester = new Sys();
    Key k("key1", 0);
    WaitAndGet ack(&k, 1,2,3);
    char* msg_serialized = ack.serialize();
    WaitAndGet* msg_two = new WaitAndGet(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Get Serialization Tests");
    delete tester;
    delete msg_two;
}

void testStatusSerialization() {
    Sys* tester = new Sys();
    Status ack("status", 1,2,3);
    char* msg_serialized = ack.serialize();
    Status* msg_two = new Status(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Status Serialization Tests");
    delete tester;
    delete msg_two;
}

void testReplySerialization() {
    Sys* tester = new Sys();
    Reply ack("status", 1,2,3);
    char* msg_serialized = ack.serialize();
    Reply* msg_two = new Reply(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Reply Serialization Tests");
    delete tester;
    delete msg_two;
}

void testRegisterSerialization() {
    Sys* tester = new Sys();
    Register ack(1,2,3, "127.0.0.1", 3001);
    char* msg_serialized = ack.serialize();
    Register* msg_two = new Register(msg_serialized);
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->OK("Passed Register Serialization Tests");
    delete tester;
    delete msg_two;
}

/**
 * Tests Directory serialization and deserialization by comparing the serialization of a Directory
 * to the serialized representation of a Directory built by deserializing the initial directory
 * 
 */ 
void testDirectorySerialization() {
    Sys* tester = new Sys();
    size_t ports[5] = {1,2,4,5};
    String* sa = new String("mama mia");
    String* sb = new String("here we");
    String* sc = new String("go");
    String* sd = new String("again");
    String* strings[4] = {sa,sb,sc,sd};
    Directory* d = new Directory(0, 1, 1, 4, ports, strings);
    char* d_serialized = d->serialize();
    Directory* d_two = new Directory(d_serialized);
    char* d_serialized_two = d_two->serialize();
    tester->t_true(strcmp(d_serialized, d_two->serialize()) == 0);
    tester->OK("Passed Directory Serialization Tests!");
    delete tester;
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    IntDistributedArray* ida = new IntDistributedArray(kvs1);
    for(int i = 0; i < 10; i++) {
        //std::cout << "here\n";
        ida->pushBack(i);
    }
    char* serialized = ida->serialize();
    IntDistributedArray* ida2 = new IntDistributedArray(serialized, kvs1);
    //std::cout << "here\n";
    system->t_true(ida->equals(ida2)); //problem child
    String* s = new String(serialized);
    String* s2 = new String(ida2->serialize());
    system->t_true(s2->equals(s));
    system->OK("Passed intDistributedArray");
    delete [] system;
}

void testStringDistributedArrays() {
    Sys* system = new Sys();
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    StringDistributedArray* sda = new StringDistributedArray(kvs1);
    StringArray* sa = genStringArray();
    for(int i = 0; i < 10; i++) {
        sda->pushBack(sa->get(i));
    }
    char* serialized = sda->serialize();
    StringDistributedArray* sda2 = new StringDistributedArray(serialized, kvs1);
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    FloatDistributedArray* fda = new FloatDistributedArray(kvs1);
    for(int i = 0; i < 40; i++) {
        fda->pushBack(i);
    }
    char* serialized = fda->serialize();   
    FloatDistributedArray* fda2 = new FloatDistributedArray(serialized, kvs1);
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    DistributedIntColumn* dfc = new DistributedIntColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    // std::cout << "here\n";
    // std::cout << serialized << "\n";   
    DistributedIntColumn* fda2 = new DistributedIntColumn(serialized, kvs1);
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    DistributedDoubleColumn* dfc = new DistributedDoubleColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedDoubleColumn* fda2 = new DistributedDoubleColumn(serialized, kvs1);
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    DistributedFloatColumn* dfc = new DistributedFloatColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedFloatColumn* fda2 = new DistributedFloatColumn(serialized, kvs1);
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    DistributedBoolColumn* dfc = new DistributedBoolColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedBoolColumn* fda2 = new DistributedBoolColumn(serialized, kvs1);
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
    DistributedStringColumn* dfc = new DistributedStringColumn(kvs1);
    for(int i = 0; i < 40; i++) {
        dfc->push_back(i);
    }
    char* serialized = dfc->serialize();
    //std::cout << serialized << "\n";   
    DistributedStringColumn* fda2 = new DistributedStringColumn(serialized, kvs1);
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
    NetworkIP network;
    KVStore* kvs1 = new KVStore(1, 0, &network);
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
    //std::cout << serialized << "\n";   
    DistributedDataFrame* dd2 = new  DistributedDataFrame(serialized, kvs1);
    String* s = new String(serialized);
    //std::cout << dd2->serialize() << "\n";
    String* s2 = new String(dd2->serialize());
    system->t_true(s2->equals(s));

    system->OK("Passed DistributedDataFrame");
    delete [] system;
}

void testApplication() {
    Sys* system = new Sys();
    NetworkIP network;
    Trivial* triv = new Trivial(0, &network);
    triv->run_();

    system->OK("Passed Application code M2");
    delete [] system;
}

void tryMap() {
    Value* v1 = new Value("muffin", 6);
    Value* v3 = new Value("carrot", 6);
    Key* k1 = new Key("payload_1", 0);
    Key* k3 = new Key("payload_1", 0);
    char* c1 =  "payload_1";
    char* c2 = "payload_3";

    std::map<Key*, Value*, KeyCompare> store;
    std::map<Key, Value>::iterator it;
    store.insert(std::pair<Key*, Value*>(k1, v1));

    std::cout << (c1 < c2)  <<"\n";
    std::cout <<  store.find(k1)->second->serialize() <<"\n";
    store.find(k3)->second = v3;   
    std::cout <<  store.find(k1)->second->serialize() <<"\n";
}



int main() {
    testValueSerialization();
    testKeySerialization();
    testIntArraySerialization();
    testIntColumnSerialization();
    testStringArraySerialization();
    testStringColumnSerialization();
    testFloatArraySerialization();
    testFloatColumnSerialization();
    testMessageSerialization();
    testAckSerialization();
    testNackSerialization();
    testPutSerialization();
    testGetSerialization();
    testWaitAndGetSerialization();
    testStatusSerialization();
    testReplySerialization();
    testRegisterSerialization();
    testDirectorySerialization();
    testIntDistributedArrays();
    testStringDistributedArrays();
    testFloatDistributedArrays();
    testDistributedIntColumn();
    testDistributedDoubleColumn();
    testDistributedFloatColumn();
    testDistributedBoolColumn();
    testDistributedStringColumn();
    testSchema();
    testDistributedDataframe();
    //tryMap();
    testApplication(); //currently fails
    return 0;
}