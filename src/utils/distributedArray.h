//lang: CwC
#pragma once
#include <stdio.h> 
#include <stdlib.h> 
#include <time.h> 
#include "../object.h"
#include "../utils/string.h"
#include "../store/kvstore.h"
#include "array.h"
#include "../store/value.h"
#include "../serialize/jsonHelper.h"
#include "../serialize/serial.h"
#include "string.h"

#include <math.h>
#include <string>

//***TODO. Push the last chunk. -> storeChunk() would work.
//***TODO. Need the other DistributedArrays except for Int.
//Authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu
/** Builds a specific type of array with similar behavior to Array class but of fixed length and Ints */
class IntDistributedArray: public Object {
    public:
    KVStore* kv_; //underlying key value store that distributes data
    IntArray* chunkArray_; //current chunk values before storing in KV
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 5; //number of elements to host in each chunk. //change to 1024 later
    size_t chunkCount_; //total number of chunks in array
    size_t itemCount_; //total number of items in array
    size_t curNode_; //current node the chunk we're adding to is located on
    size_t totalNodes_; //total number of nodes in system

    IntDistributedArray(KVStore* kv) {
        kv_ = kv;
        totalNodes_ = kv->num_nodes_;
        srand(time(nullptr));
        uid_ = rand();
        curNode_ = 0;
        itemCount_ = 0;
        chunkCount_ = 0;
        keys_ = new Array();
        chunkArray_ = new IntArray();
    }

    IntDistributedArray(char* serialized) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        //std::cout<<payload<<"\n";
        this->chunkSize_ = std::stoi(JSONHelper::getValueFromKey("chunkSize_", payload)->c_str());
        this->totalNodes_ = std::stoi(JSONHelper::getValueFromKey("totalNodes_", payload)->c_str());
        this->curNode_ = std::stoi(JSONHelper::getValueFromKey("curNode_", payload)->c_str());
        this->uid_ = std::stoi(JSONHelper::getValueFromKey("uid_", payload)->c_str());
        this->itemCount_ = std::stoi(JSONHelper::getValueFromKey("itemCount_", payload)->c_str());
        this->chunkCount_ = std::stoi(JSONHelper::getValueFromKey("chunkCount_", payload)->c_str());
        this->keys_ = new Array(JSONHelper::getValueFromKey("keys_", payload)->c_str());
        this->kv_ = new KVStore(JSONHelper::getValueFromKey("kv_", payload)->c_str());
        this->chunkArray_ = new IntArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   int get(size_t index) {
       //new logic to check if current chunk
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->get(index % chunkSize_);
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_)))); //gets value of given key
        char* serlializedNode = v->data;
        IntArray* intVals = new IntArray(serlializedNode); //turns payload into IntArray object
        return intVals->get(index % chunkSize_); //returns int at correct index in array at this chunk
    };
    /*
    * Given an index, updates the value at that index
    */
  int set(size_t index, int val) {
      //new logic to check if current chunk
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->set(index % chunkSize_, val);
        }
        Key* curKey = dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_))); //gets key of given characteristics
        Value* v = kv_->get(curKey); //use curKey to get value from kvStore
        char* serlializedNode = v->data;
        char* payload = JSONHelper::getPayloadValue(serlializedNode)->c_str(); //returns the payload (IntArray) of serialized data
        IntArray* intVals = new IntArray(payload); //turns payload into IntArray object

        int prevVal = intVals->get(index % chunkSize_); //get int at given index for this array in this chunk
        intVals->set((index % chunkSize_), val); //sets index of this array to provided val
        kv_->put(curKey, new Value(intVals->serialize(), 0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
    
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff* sb = new StrBuff();
        sb->c(uid_);
        sb->c("_dist_int_array_chunk_");
        sb->c(chunkCount_);
        String* s = sb->get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), 0)); //adds new IntArray to kvStore
        chunkArray_ = new IntArray(); //creates new Int array to store values for new chunk
        curNode_+=1;
        chunkCount_+=1;
        if(curNode_ == totalNodes_) { //starts cycle of chunks on nodes again
            curNode_ = 0;
        }
    }
    /*
    *   Adds a new int to the back of the Distributed Int Array
    */ 
    void pushBack(int val) {
        //initialize first key
        if(itemCount_ % chunkSize_ == 0 && itemCount_ != 0) { //if current chunk is full..
            storeChunk(); //stores the previous chunk values in the kv_store
            chunkArray_->pushBack(val);
        } else {
            chunkArray_->pushBack(val);
        }
        itemCount_ +=1;
    }; //add o to end of array

  bool empty() {
      return itemCount_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        IntDistributedArray *s = dynamic_cast<IntDistributedArray*>(o);
        if(s == nullptr) return false;
        if(itemCount_ != s->itemCount_) return false;
        for(int i = 0; i < itemCount_; i++) {
            bool sameInt = s->get(i) == get(i);
            if(!sameInt) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return itemCount_;
  }; //returns the number of elements in the array

  /** Return the hash value of this object */
  size_t hash_me() {
      int temp = 0;
      for(int i = 0; i < itemCount_; i++) {
          temp *= get(i);
      }
      return temp;
  }
  char* serialize() {
      Serializable* sb = new Serializable();
      sb->initSerialize("IntDistributedArray");
      sb->write("uid_", uid_);
      sb->write("chunkSize_", chunkSize_);
      sb->write("chunkCount_", chunkCount_);
      sb->write("itemCount_", itemCount_);
      sb->write("curNode_", curNode_);
      sb->write("totalNodes_", totalNodes_);
      sb->write("keys_", keys_->serialize(), false);
      sb->write("kv_", kv_->serialize(), false);
      sb->write("chunkArray_", chunkArray_->serialize(), false);
      sb->endSerialize();
      char* value = sb->get();
      delete sb;
      return value;
  }//serializes this distributed Array
};

class FloatDistributedArray: public Object {
    public:
    KVStore* kv_; //underlying key value store that distributes data
    FloatArray* chunkArray_; //current chunk values before storing in KV
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 50; //number of elements to host in each chunk
    size_t chunkCount_; //total number of chunks in array
    size_t itemCount_; //total number of items in array
    size_t curNode_; //current node the chunk we're adding to is located on
    size_t totalNodes_; //total number of nodes in system

    FloatDistributedArray(KVStore* kv) {
        kv_ = kv;
        totalNodes_ = kv->num_nodes_;
        srand(time(nullptr));
        uid_ = rand();
        curNode_ = 0;
        itemCount_ = 0;
        chunkCount_ = 0;
        keys_ = new Array();
        chunkArray_ = new FloatArray();
    }

    FloatDistributedArray(char* serialized) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->chunkSize_ = std::stoi(JSONHelper::getValueFromKey("chunkSize_", payload)->c_str());
        this->totalNodes_ = std::stoi(JSONHelper::getValueFromKey("totalNodes_", payload)->c_str());
        this->curNode_ = std::stoi(JSONHelper::getValueFromKey("curNode_", payload)->c_str());
        this->uid_ = std::stoi(JSONHelper::getValueFromKey("uid_", payload)->c_str());
        this->itemCount_ = std::stoi(JSONHelper::getValueFromKey("itemCount_", payload)->c_str());
        this->chunkCount_ = std::stoi(JSONHelper::getValueFromKey("chunkCount_", payload)->c_str());
        this->keys_ = new Array(JSONHelper::getValueFromKey("keys_", payload)->c_str());
        this->kv_ = new KVStore(JSONHelper::getValueFromKey("kv_", payload)->c_str());
        this->chunkArray_ = new FloatArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   float get(size_t index) {
       //new logic to check if current chunk
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->get(index % chunkSize_);
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_)))); //gets value of given key
        char* serlializedNode = v->data; 
        FloatArray* floatVals = new FloatArray(serlializedNode); //turns payload into IntArray object
        return floatVals->get(index % chunkSize_); //returns int at correct index in array at this chunk
    };
    /*
    * Given an index, updates the value at that index
    */
  float set(size_t index, float val) {
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->set(index % chunkSize_, val);
        }
        Key* curKey = dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_))); //gets key of given characteristics
        Value* v = kv_->get(curKey); //use curKey to get value from kvStore
        char* serlializedNode = v->data;
        char* payload = JSONHelper::getPayloadValue(serlializedNode)->c_str(); //returns the payload (FloatArray) of serialized data
        FloatArray* floatVals = new FloatArray(payload); //turns payload into FloatArray object

        float prevVal = floatVals->get(index % chunkSize_); //get int at given index for this array in this chunk
        floatVals->set((index % chunkSize_), val); //sets index of this array to provided val
        kv_->put(curKey, new Value(floatVals->serialize(), 0)); //reserialize FloatArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
  
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff* sb = new StrBuff();
        sb->c(uid_);
        sb->c("_dist_int_array_chunk_");
        sb->c(chunkCount_);
        String* s = sb->get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), 0)); //adds new IntArray to kvStore
        chunkArray_ = new FloatArray(); //creates new Int array to store values for new chunk
        curNode_+=1;
        chunkCount_+=1;
        if(curNode_ == totalNodes_) { //starts cycle of chunks on nodes again
            curNode_ = 0;
        }
    }    
    /*
    *   Adds a new int to the back of the Distributed Int Array
    */ 
    void pushBack(float val) {
        //initialize first key
        if(itemCount_ % chunkSize_ == 0 && itemCount_ != 0) { //if current chunk is full..
            storeChunk(); //stores the previous chunk values in the kv_store
            chunkArray_->pushBack(val);
        } else {
            chunkArray_->pushBack(val);
        }
        itemCount_ +=1;
    }; //add o to end of array

  bool empty() {
      return itemCount_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        FloatDistributedArray *s = dynamic_cast<FloatDistributedArray*>(o);
        if(s == nullptr) return false;
        if(itemCount_ != s->itemCount_) return false;
        for(int i = 0; i < itemCount_; i++) {
            bool sameInt = s->get(i) == get(i);
            if(!sameInt) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return itemCount_;
  }; //returns the number of elements in the array

  /** Return the hash value of this object */
  size_t hash_me() {
      int temp = 0;
      for(int i = 0; i < itemCount_; i++) {
          temp *= get(i);
      }
      return temp;
  }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("FloatDistributedArray");
        sb->write("uid_", uid_);
        sb->write("chunkSize_", chunkSize_);
        sb->write("chunkCount_", chunkCount_);
        sb->write("itemCount_", itemCount_);
        sb->write("curNode_", curNode_);
        sb->write("totalNodes_", totalNodes_);
        sb->write("keys_", keys_->serialize(), false);
        sb->write("kv_", kv_->serialize(), false);
        sb->write("chunkArray_", chunkArray_->serialize(), false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }//serializes this distributed Array
};

class StringDistributedArray: public Object {
    public:
    KVStore* kv_; //underlying key value store that distributes data
    StringArray* chunkArray_; //current chunk values before storing in KV
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 50; //number of elements to host in each chunk
    size_t chunkCount_; //total number of chunks in array
    size_t itemCount_; //total number of items in array
    size_t curNode_; //current node the chunk we're adding to is located on
    size_t totalNodes_; //total number of nodes in system

    StringDistributedArray(KVStore* kv) {
        kv_ = kv;
        totalNodes_ = kv->num_nodes_;
        srand(time(nullptr));
        uid_ = rand();
        curNode_ = 0;
        itemCount_ = 0;
        chunkCount_ = 0;
        keys_ = new Array();
        chunkArray_ = new StringArray();
    }

    StringDistributedArray(char* serialized) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        //std::cout<<payload<<"\n";
        this->chunkSize_ = std::stoi(JSONHelper::getValueFromKey("chunkSize_", payload)->c_str());
        this->totalNodes_ = std::stoi(JSONHelper::getValueFromKey("totalNodes_", payload)->c_str());
        this->curNode_ = std::stoi(JSONHelper::getValueFromKey("curNode_", payload)->c_str());
        this->uid_ = std::stoi(JSONHelper::getValueFromKey("uid_", payload)->c_str());
        this->itemCount_ = std::stoi(JSONHelper::getValueFromKey("itemCount_", payload)->c_str());
        this->chunkCount_ = std::stoi(JSONHelper::getValueFromKey("chunkCount_", payload)->c_str());
        this->keys_ = new Array(JSONHelper::getValueFromKey("keys_", payload)->c_str());
        this->kv_ = new KVStore(JSONHelper::getValueFromKey("kv_", payload)->c_str());
        this->chunkArray_ = new StringArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   String* get(size_t index) {
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->get(index % chunkSize_);
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_)))); //gets value of given key
        char* serlializedNode = v->data; 
        StringArray* strVals = new StringArray(serlializedNode); //turns payload into IntArray object
        return strVals->get(index % chunkSize_); //returns int at correct index in array at this chunk
    };
    /*
    * Given an index, updates the value at that index
    */
  String* set(size_t index, String* val) {
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->set(index % chunkSize_, val);
        }
        Key* curKey = dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_))); //gets key of given characteristics
        Value* v = kv_->get(curKey); //use curKey to get value from kvStore
        char* serlializedNode = v->data;
        char* payload = JSONHelper::getPayloadValue(serlializedNode)->c_str(); //returns the payload (IntArray) of serialized data
        StringArray* strVals = new StringArray(payload); //turns payload into IntArray object

        String* prevVal = strVals->get(index % chunkSize_); //get int at given index for this array in this chunk
        strVals->set((index % chunkSize_), val); //sets index of this array to provided val
        kv_->put(curKey, new Value(strVals->serialize(), 0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
    
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff* sb = new StrBuff();
        sb->c(uid_);
        sb->c("_dist_int_array_chunk_");
        sb->c(chunkCount_);
        String* s = sb->get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), 0)); //adds new IntArray to kvStore
        chunkArray_ = new StringArray(); //creates new Int array to store values for new chunk
        curNode_+=1;
        chunkCount_+=1;
        if(curNode_ == totalNodes_) { //starts cycle of chunks on nodes again
            curNode_ = 0;
        }
    }
    /*
    *   Adds a new int to the back of the Distributed Int Array
    */ 
    void pushBack(String* val) {
        //initialize first key
        if(itemCount_ % chunkSize_ == 0 && itemCount_ != 0) { //if current chunk is full..
            storeChunk(); //stores the previous chunk values in the kv_store
            chunkArray_->pushBack(val);
        } else {
            chunkArray_->pushBack(val);
        }
        itemCount_ +=1;
    }; //add o to end of array

  bool empty() {
      return itemCount_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        StringDistributedArray *s = dynamic_cast<StringDistributedArray*>(o);
        if(s == nullptr) return false;
        if(itemCount_ != s->itemCount_) return false;
        for(int i = 0; i < itemCount_; i++) {
            bool sameInt = s->get(i)->equals(get(i));
            if(!sameInt) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return itemCount_;
  }; //returns the number of elements in the array

    /** Return the hash value of this object */
  size_t hash_me() {
      size_t temp = 0;
      for(int i = 0; i < itemCount_; i++) {
          temp *= get(i)->hash();
      }
      return temp;
  }

  char* serialize() {
      Serializable* sb = new Serializable();
      sb->initSerialize("StringDistributedArray");
      sb->write("uid_", uid_);
      sb->write("chunkSize_", chunkSize_);
      sb->write("chunkCount_", chunkCount_);
      sb->write("itemCount_", itemCount_);
      sb->write("curNode_", curNode_);
      sb->write("totalNodes_", totalNodes_);
      sb->write("keys_", keys_->serialize(), false);
      sb->write("kv_", kv_->serialize(), false);
      sb->write("chunkArray_", chunkArray_->serialize(), false);
      sb->endSerialize();
      char* value = sb->get();
      delete sb;
      return value;
  }//serializes this distributed Array
};

class BoolDistributedArray: public Object {
    public:
    KVStore* kv_; //underlying key value store that distributes data
    BoolArray* chunkArray_; //current chunk values before storing in KV
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 50; //number of elements to host in each chunk
    size_t chunkCount_; //total number of chunks in array
    size_t itemCount_; //total number of items in array
    size_t curNode_; //current node the chunk we're adding to is located on
    size_t totalNodes_; //total number of nodes in system

    BoolDistributedArray(KVStore* kv) {
        kv_ = kv;
        totalNodes_ = kv->num_nodes_;
        srand(time(nullptr));
        uid_ = rand();
        curNode_ = 0;
        itemCount_ = 0;
        chunkCount_ = 0;
        keys_ = new Array();
        chunkArray_ = new BoolArray();
    }

    BoolDistributedArray(char* serialized) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        //std::cout<<payload<<"\n";
        this->chunkSize_ = std::stoi(JSONHelper::getValueFromKey("chunkSize_", payload)->c_str());
        this->totalNodes_ = std::stoi(JSONHelper::getValueFromKey("totalNodes_", payload)->c_str());
        this->curNode_ = std::stoi(JSONHelper::getValueFromKey("curNode_", payload)->c_str());
        this->uid_ = std::stoi(JSONHelper::getValueFromKey("uid_", payload)->c_str());
        this->itemCount_ = std::stoi(JSONHelper::getValueFromKey("itemCount_", payload)->c_str());
        this->chunkCount_ = std::stoi(JSONHelper::getValueFromKey("chunkCount_", payload)->c_str());
        this->keys_ = new Array(JSONHelper::getValueFromKey("keys_", payload)->c_str());
        this->kv_ = new KVStore(JSONHelper::getValueFromKey("kv_", payload)->c_str());
        this->chunkArray_ = new BoolArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   bool get(size_t index) {
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->get(index % chunkSize_);
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_)))); //gets value of given key
        char* serlializedNode = v->data; 
        BoolArray* strVals = new BoolArray(serlializedNode); //turns payload into IntArray object
        return strVals->get(index % chunkSize_); //returns int at correct index in array at this chunk
    };
    /*
    * Given an index, updates the value at that index
    */
  bool set(size_t index, bool val) {
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->set(index % chunkSize_, val);
        }
        Key* curKey = dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_))); //gets key of given characteristics
        Value* v = kv_->get(curKey); //use curKey to get value from kvStore
        char* serlializedNode = v->data;
        char* payload = JSONHelper::getPayloadValue(serlializedNode)->c_str(); //returns the payload (IntArray) of serialized data
        BoolArray* strVals = new BoolArray(payload); //turns payload into IntArray object

        bool prevVal = strVals->get(index % chunkSize_); //get int at given index for this array in this chunk
        strVals->set((index % chunkSize_), val); //sets index of this array to provided val
        kv_->put(curKey, new Value(strVals->serialize(), 0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object

    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff* sb = new StrBuff();
        sb->c(uid_);
        sb->c("_dist_int_array_chunk_");
        sb->c(chunkCount_);
        String* s = sb->get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), 0)); //adds new IntArray to kvStore
        chunkArray_ = new BoolArray(); //creates new Int array to store values for new chunk
        curNode_+=1;
        chunkCount_+=1;
        if(curNode_ == totalNodes_) { //starts cycle of chunks on nodes again
            curNode_ = 0;
        }
    }
    /*
    *   Adds a new int to the back of the Distributed Int Array
    */ 
    void pushBack(bool val) {
        //initialize first key
        if(itemCount_ % chunkSize_ == 0 && itemCount_ != 0) { //if current chunk is full..
            storeChunk(); //stores the previous chunk values in the kv_store
            chunkArray_->pushBack(val); 
        } else {
            chunkArray_->pushBack(val);
        }
        itemCount_ +=1;
    }; //add o to end of array

  bool empty() {
      return itemCount_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        BoolDistributedArray *s = dynamic_cast<BoolDistributedArray*>(o);
        if(s == nullptr) return false;
        if(itemCount_ != s->itemCount_) return false;
        for(int i = 0; i < itemCount_; i++) {
            bool sameInt = s->get(i) == (get(i));
            if(!sameInt) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return itemCount_;
  }; //returns the number of elements in the array

    /** Return the hash value of this object */
  size_t hash_me() {
      size_t temp = 0;
      for(int i = 0; i < itemCount_; i++) {
          temp *= get(i);
      }
      return temp;
  }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("BoolDistributedArray");
        sb->write("uid_", uid_);
        sb->write("chunkSize_", chunkSize_);
        sb->write("chunkCount_", chunkCount_);
        sb->write("itemCount_", itemCount_);
        sb->write("curNode_", curNode_);
        sb->write("totalNodes_", totalNodes_);
        sb->write("keys_", keys_->serialize(), false);
        sb->write("kv_", kv_->serialize(), false);
        sb->write("chunkArray_", chunkArray_->serialize(), false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }//serializes this distributed Array

};

class DoubleDistributedArray: public Object {
    public:
    KVStore* kv_; //underlying key value store that distributes data
    DoubleArray* chunkArray_; //current chunk values before storing in KV
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 50; //number of elements to host in each chunk
    size_t chunkCount_; //total number of chunks in array
    size_t itemCount_; //total number of items in array
    size_t curNode_; //current node the chunk we're adding to is located on
    size_t totalNodes_; //total number of nodes in system

    DoubleDistributedArray(KVStore* kv) {
        kv_ = kv;
        totalNodes_ = kv->num_nodes_;
        srand(time(nullptr));
        uid_ = rand();
        curNode_ = 0;
        itemCount_ = 0;
        chunkCount_ = 0;
        keys_ = new Array();
        chunkArray_ = new DoubleArray();
    }

    DoubleDistributedArray(char* serialized) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        //std::cout<<payload<<"\n";
        this->chunkSize_ = std::stoi(JSONHelper::getValueFromKey("chunkSize_", payload)->c_str());
        this->totalNodes_ = std::stoi(JSONHelper::getValueFromKey("totalNodes_", payload)->c_str());
        this->curNode_ = std::stoi(JSONHelper::getValueFromKey("curNode_", payload)->c_str());
        this->uid_ = std::stoi(JSONHelper::getValueFromKey("uid_", payload)->c_str());
        this->itemCount_ = std::stoi(JSONHelper::getValueFromKey("itemCount_", payload)->c_str());
        this->chunkCount_ = std::stoi(JSONHelper::getValueFromKey("chunkCount_", payload)->c_str());
        this->keys_ = new Array(JSONHelper::getValueFromKey("keys_", payload)->c_str());
        this->kv_ = new KVStore(JSONHelper::getValueFromKey("kv_", payload)->c_str());
        this->chunkArray_ = new DoubleArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   double get(size_t index) {
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->get(index % chunkSize_);
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_)))); //gets value of given key
        char* serlializedNode = v->data; 
        DoubleArray* strVals = new DoubleArray(serlializedNode); //turns payload into IntArray object
        return strVals->get(index % chunkSize_); //returns int at correct index in array at this chunk
    };
    /*
    * Given an index, updates the value at that index
    */
  double set(size_t index, double val) {
        assert(index < itemCount_);
        if(index/chunkSize_ == chunkCount_) {
            return chunkArray_->set(index % chunkSize_, val);
        }
        Key* curKey = dynamic_cast<Key*>(keys_->get(floor((index/chunkSize_) % totalNodes_))); //gets key of given characteristics
        Value* v = kv_->get(curKey); //use curKey to get value from kvStore
        char* serlializedNode = v->data;
        char* payload = JSONHelper::getPayloadValue(serlializedNode)->c_str(); //returns the payload (IntArray) of serialized data
        DoubleArray* strVals = new DoubleArray(payload); //turns payload into IntArray object

        double prevVal = strVals->get(index % chunkSize_); //get int at given index for this array in this chunk
        strVals->set((index % chunkSize_), val); //sets index of this array to provided val
        kv_->put(curKey, new Value(strVals->serialize(), 0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
    
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff* sb = new StrBuff();
        sb->c(uid_);
        sb->c("_dist_int_array_chunk_");
        sb->c(chunkCount_);
        String* s = sb->get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), 0)); //adds new IntArray to kvStore
        chunkArray_ = new DoubleArray(); //creates new Int array to store values for new chunk
        curNode_+=1;
        chunkCount_+=1;
        if(curNode_ == totalNodes_) { //starts cycle of chunks on nodes again
            curNode_ = 0;
        }
    }
    /*
    *   Adds a new int to the back of the Distributed Int Array
    */ 
    void pushBack(bool val) {
        //initialize first key
        if(itemCount_ % chunkSize_ == 0 && itemCount_ != 0) { //if current chunk is full..
            storeChunk(); //stores the previous chunk values in the kv_store
            chunkArray_->pushBack(val);
        } else {
            chunkArray_->pushBack(val);
        }
        itemCount_ +=1;
    }; //add o to end of array

  bool empty() {
      return itemCount_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        DoubleDistributedArray *s = dynamic_cast<DoubleDistributedArray*>(o);
        if(s == nullptr) return false;
        if(itemCount_ != s->itemCount_) return false;
        for(int i = 0; i < itemCount_; i++) {
            bool sameInt = s->get(i) == (get(i));
            if(!sameInt) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return itemCount_;
  }; //returns the number of elements in the array

      /** Return the hash value of this object */
  size_t hash_me() {
      size_t temp = 0;
      for(int i = 0; i < itemCount_; i++) {
          temp *= get(i);
      }
      return temp;
  }

    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("DoubleDistributedArray");
        sb->write("uid_", uid_);
        sb->write("chunkSize_", chunkSize_);
        sb->write("chunkCount_", chunkCount_);
        sb->write("itemCount_", itemCount_);
        sb->write("curNode_", curNode_);
        sb->write("totalNodes_", totalNodes_);
        sb->write("keys_", keys_->serialize(), false);
        sb->write("kv_", kv_->serialize(), false);
        sb->write("chunkArray_", chunkArray_->serialize(), false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }//serializes this distributed Array

};