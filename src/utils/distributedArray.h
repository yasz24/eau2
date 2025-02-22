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
    IntArray** cache_ = nullptr; // a local cache for chunk_arrays
    size_t cache_count_ = 0; // number of chunk arrays in the cache.
    size_t max_cache_count = 50; //upper bound on number of chunk arrays to cache in memory  
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 1024; //number of elements to host in each chunk. //change to 1024 later
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
        chunkArray_ = new IntArray(chunkSize_);
    }

    IntDistributedArray(char* serialized, KVStore* kv) {
        //std::cout << serialized <<"\n";
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
        this->kv_ = kv;
        this->chunkArray_ = new IntArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   int get(size_t index) {
       //new logic to check if current chunk
        assert(index < itemCount_);
        //std::cout << "in get " << index <<"\n";
        if (cache_ == nullptr) {
            init_cache_();
        }
        int res;
        if(index/chunkSize_ == chunkCount_) {
            res = chunkArray_->get(index % chunkSize_);
        } else {

            size_t key_idx = floor((index/chunkSize_));
            //std::cout << "key index: " << key_idx << "\n";
            //std::cout << key
            IntArray* doubleVals;
            if (cache_[key_idx] != nullptr) {
                //std::cout << "get chunk from cache\n";
                doubleVals = cache_[key_idx];
            } else {
                //std::cout << "get chunk from kv\n";
                doubleVals = get_chunk_from_kv_(key_idx);
            }
            res = doubleVals->get(index % chunkSize_);
        }
        //std::cout << "res: " << res << "\n";
        return res; //returns int at correct index in array at this chunk
    };

    //make call to KVStore. Put item in cache. increment cache count.
    IntArray* get_chunk_from_kv_(size_t key_idx) {
        if (cache_count_ >= max_cache_count) {
            reset_cache_();
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(key_idx))); //gets value of given key
        char* serlializedNode = v->data; 
        //std::cout << serlializedNode <<"\n";
        IntArray* doubleVals = new IntArray(serlializedNode); //turns payload into IntArray object
        cache_[key_idx] = doubleVals;
        cache_count_ += 1;
        return doubleVals;
    }

    //free all the pointers in cache. reset cache_count_ to 0.
    void reset_cache_() {
        //std::cout << "reset cache, cache count: " << cache_count_ << "\n";
        for (size_t i = 0; i < keys_->length(); i++) {
            if (cache_[i] != nullptr) {
                //std::cout << "clearing idx: " << i<<"\n";
                delete cache_[i];
                cache_[i] = nullptr;
            }
        }
        cache_count_ = 0;
    }

    void init_cache_() {
        //assuming a dataframe is immutable, so nu pushbacks after the first get call.
        cache_ = new IntArray*[keys_->length()];
        for (size_t i = 0; i < keys_->length(); i++) {
            cache_[i] = nullptr;
        }
        return;
    }
    /*
    * Given an index, updates the value at that index
    */
  int set(size_t index, int val) {
      //new logic to check if current chunk
       assert(index <= itemCount_);
        if(index == itemCount_) {
            pushBack(val);
            return val;
        }
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
        kv_->put(curKey, new Value(intVals->serialize(), (size_t)0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
    
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff sb;
        sb.c(uid_);
        sb.c("_dist_int_array_chunk_");
        sb.c(chunkCount_);
        String* s = sb.get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), (size_t)0)); //adds new IntArray to kvStore
        //std::cout << "in store chunk\n";
        delete chunkArray_;
        chunkArray_ = new IntArray(chunkSize_); //creates new Int array to store values for new chunk
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
    FloatArray** cache_ = nullptr; // a local cache for chunk_arrays
    size_t cache_count_ = 0; // number of chunk arrays in the cache.
    size_t max_cache_count = 50; //upper bound on number of chunk arrays to cache in memory  
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 1024; //number of elements to host in each chunk
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
        chunkArray_ = new FloatArray(chunkSize_);
    }

    FloatDistributedArray(char* serialized, KVStore* kv) {
        Deserializable* ds = new Deserializable();
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->chunkSize_ = std::stoi(JSONHelper::getValueFromKey("chunkSize_", payload)->c_str());
        this->totalNodes_ = std::stoi(JSONHelper::getValueFromKey("totalNodes_", payload)->c_str());
        this->curNode_ = std::stoi(JSONHelper::getValueFromKey("curNode_", payload)->c_str());
        this->uid_ = std::stoi(JSONHelper::getValueFromKey("uid_", payload)->c_str());
        this->itemCount_ = std::stoi(JSONHelper::getValueFromKey("itemCount_", payload)->c_str());
        this->chunkCount_ = std::stoi(JSONHelper::getValueFromKey("chunkCount_", payload)->c_str());
        this->keys_ = new Array(JSONHelper::getValueFromKey("keys_", payload)->c_str());
        this->kv_ = kv;
        //this->kv_ = new KVStore(JSONHelper::getValueFromKey("kv_", payload)->c_str());
        this->chunkArray_ = new FloatArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   float get(size_t index) {
       //new logic to check if current chunk
        assert(index < itemCount_);
        //std::cout << "in get " << index <<"\n";
        if (cache_ == nullptr) {
            init_cache_();
        }
        float res;
        if(index/chunkSize_ == chunkCount_) {
            res = chunkArray_->get(index % chunkSize_);
        } else {

            size_t key_idx = floor((index/chunkSize_));
            //std::cout << "key index: " << key_idx << "\n";
            //std::cout << key
            FloatArray* doubleVals;
            if (cache_[key_idx] != nullptr) {
                //std::cout << "get chunk from cache\n";
                doubleVals = cache_[key_idx];
            } else {
                //std::cout << "get chunk from kv\n";
                doubleVals = get_chunk_from_kv_(key_idx);
            }
            res = doubleVals->get(index % chunkSize_);
        }
        //std::cout << "res: " << res << "\n";
        return res; //returns int at correct index in array at this chunk
    };

    //make call to KVStore. Put item in cache. increment cache count.
    FloatArray* get_chunk_from_kv_(size_t key_idx) {
        if (cache_count_ >= max_cache_count) {
            reset_cache_();
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(key_idx))); //gets value of given key
        char* serlializedNode = v->data; 
        //std::cout << serlializedNode <<"\n";
        FloatArray* doubleVals = new FloatArray(serlializedNode); //turns payload into IntArray object
        cache_[key_idx] = doubleVals;
        cache_count_ += 1;
        return doubleVals;
    }

    //free all the pointers in cache. reset cache_count_ to 0.
    void reset_cache_() {
        //std::cout << "reset cache, cache count: " << cache_count_ << "\n";
        for (size_t i = 0; i < keys_->length(); i++) {
            if (cache_[i] != nullptr) {
                //std::cout << "clearing idx: " << i<<"\n";
                delete cache_[i];
                cache_[i] = nullptr;
            }
        }
        cache_count_ = 0;
    }

    void init_cache_() {
        //assuming a dataframe is immutable, so nu pushbacks after the first get call.
        cache_ = new FloatArray*[keys_->length()];
        for (size_t i = 0; i < keys_->length(); i++) {
            cache_[i] = nullptr;
        }
        return;
    }

    /*
    * Given an index, updates the value at that index
    */
  float set(size_t index, float val) {
        assert(index <= itemCount_);
        if(index == itemCount_) {
            pushBack(val);
            return val;
        }
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
        kv_->put(curKey, new Value(floatVals->serialize(), (size_t)0)); //reserialize FloatArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
  
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff sb;
        sb.c(uid_);
        sb.c("_dist_float_array_chunk_");
        sb.c(chunkCount_);
        String* s = sb.get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), (size_t)0)); //adds new IntArray to kvStore
        delete chunkArray_;
        chunkArray_ = new FloatArray(chunkSize_);         
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
    StringArray** cache_ = nullptr; // a local cache for chunk_arrays
    size_t cache_count_ = 0; // number of chunk arrays in the cache.
    size_t max_cache_count = 50; //upper bound on number of chunk arrays to cache in memory  
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 1024; //number of elements to host in each chunk
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
        chunkArray_ = new StringArray(chunkSize_);
    }

    StringDistributedArray(char* serialized, KVStore* kv) {
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
        this->kv_ = kv;
        this->chunkArray_ = new StringArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   String* get(size_t index) {
        assert(index < itemCount_);
        //std::cout << "in get " << index <<"\n";
        if (cache_ == nullptr) {
            init_cache_();
        }
        String* res;
        if(index/chunkSize_ == chunkCount_) {
            res = chunkArray_->get(index % chunkSize_);
        } else {

            size_t key_idx = floor((index/chunkSize_));
            //std::cout << "key index: " << key_idx << "\n";
            //std::cout << key
            StringArray* doubleVals;
            if (cache_[key_idx] != nullptr) {
                //std::cout << "get chunk from cache\n";
                doubleVals = cache_[key_idx];
            } else {
                //std::cout << "get chunk from kv\n";
                doubleVals = get_chunk_from_kv_(key_idx);
            }
            res = doubleVals->get(index % chunkSize_);
        }
        //std::cout << "res: " << res << "\n";
        return res; //returns int at correct index in array at this chunk
    };
    
     //make call to KVStore. Put item in cache. increment cache count.
    StringArray* get_chunk_from_kv_(size_t key_idx) {
        if (cache_count_ >= max_cache_count) {
            reset_cache_();
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(key_idx))); //gets value of given key
        char* serlializedNode = v->data; 
        //std::cout << serlializedNode <<"\n";
        StringArray* doubleVals = new StringArray(serlializedNode); //turns payload into IntArray object
        cache_[key_idx] = doubleVals;
        cache_count_ += 1;
        return doubleVals;
    }

    //free all the pointers in cache. reset cache_count_ to 0.
    void reset_cache_() {
        //std::cout << "reset cache, cache count: " << cache_count_ << "\n";
        for (size_t i = 0; i < keys_->length(); i++) {
            if (cache_[i] != nullptr) {
                //std::cout << "clearing idx: " << i<<"\n";
                delete cache_[i];
                cache_[i] = nullptr;
            }
        }
        cache_count_ = 0;
    }

    void init_cache_() {
        //assuming a dataframe is immutable, so nu pushbacks after the first get call.
        cache_ = new StringArray*[keys_->length()];
        for (size_t i = 0; i < keys_->length(); i++) {
            cache_[i] = nullptr;
        }
        return;
    }

    /*
    * Given an index, updates the value at that index
    */
  String* set(size_t index, String* val) {
        assert(index <= itemCount_);
        if(index == itemCount_) {
            pushBack(val);
            return val;
        }
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
        kv_->put(curKey, new Value(strVals->serialize(), (size_t)0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
    
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff sb;
        sb.c(uid_);
        sb.c("_dist_string_array_chunk_");
        sb.c(chunkCount_);
        String* s = sb.get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), (size_t)0)); //adds new IntArray to kvStore
        delete chunkArray_;
        chunkArray_ = new StringArray(chunkSize_); 
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
    BoolArray** cache_ = nullptr; // a local cache for chunk_arrays
    size_t cache_count_ = 0; // number of chunk arrays in the cache.
    size_t max_cache_count = 50; //upper bound on number of chunk arrays to cache in memory  
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 1024; //number of elements to host in each chunk
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
        chunkArray_ = new BoolArray(chunkSize_);
    }

    BoolDistributedArray(char* serialized, KVStore* kv) {
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
        this->kv_ = kv;
        //this->kv_ = new KVStore(JSONHelper::getValueFromKey("kv_", payload)->c_str());
        this->chunkArray_ = new BoolArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
   bool get(size_t index) {
        assert(index < itemCount_);
        //std::cout << "in get " << index <<"\n";
        if (cache_ == nullptr) {
            init_cache_();
        }
        bool res;
        if(index/chunkSize_ == chunkCount_) {
            res = chunkArray_->get(index % chunkSize_);
        } else {
            size_t key_idx = floor((index/chunkSize_));
            //std::cout << "key index: " << key_idx << "\n";
            //std::cout << key
            BoolArray* boolVals;
            if (cache_[key_idx] != nullptr) {
                //std::cout << "get chunk from cache\n";
                boolVals = cache_[key_idx];
            } else {
                //std::cout << "get chunk from kv\n";
                boolVals = get_chunk_from_kv_(key_idx);
            }
            res = boolVals->get(index % chunkSize_);
        }
        //std::cout << "res: " << res << "\n";
        return res; //returns int at correct index in array at this chunk
    };
    
    //make call to KVStore. Put item in cache. increment cache count.
    BoolArray* get_chunk_from_kv_(size_t key_idx) {
        if (cache_count_ >= max_cache_count) {
            reset_cache_();
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(key_idx))); //gets value of given key
        char* serlializedNode = v->data; 
        //std::cout << serlializedNode <<"\n";
        BoolArray* boolVals = new BoolArray(serlializedNode); //turns payload into IntArray object
        cache_[key_idx] = boolVals;
        cache_count_ += 1;
        return boolVals;
    }

    //free all the pointers in cache. reset cache_count_ to 0.
    void reset_cache_() {
        //std::cout << "reset cache, cache count: " << cache_count_ << "\n";
        for (size_t i = 0; i < keys_->length(); i++) {
            if (cache_[i] != nullptr) {
                //std::cout << "clearing idx: " << i<<"\n";
                delete cache_[i];
                cache_[i] = nullptr;
            }
        }
        cache_count_ = 0;
    }

    void init_cache_() {
        //assuming a dataframe is immutable, so nu pushbacks after the first get call.
        cache_ = new BoolArray*[keys_->length()];
        for (size_t i = 0; i < keys_->length(); i++) {
            cache_[i] = nullptr;
        }
        return;
    }

    /*
    * Given an index, updates the value at that index
    */
  bool set(size_t index, bool val) {
        assert(index <= itemCount_);
        if(index == itemCount_) {
            pushBack(val);
            return val;
        }
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
        kv_->put(curKey, new Value(strVals->serialize(), (size_t)0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object

    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff sb;
        sb.c(uid_);
        sb.c("_dist_bool_array_chunk_");
        sb.c(chunkCount_);
        String* s = sb.get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        kv_->put(k, new Value(chunkArray_->serialize(), (size_t)0)); //adds new IntArray to kvStore
        delete chunkArray_;
        chunkArray_ = new BoolArray(chunkSize_); 
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
        sb->write("chunkArray_", chunkArray_->serialize(), false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }//serializes this distributed Array

};

//todo: set on chunkarray_
class DoubleDistributedArray: public Object {
    public:
    KVStore* kv_; //underlying key value store that distributes data
    DoubleArray* chunkArray_; //current chunk values before storing in KV
    DoubleArray** cache_ = nullptr; // a local cache for chunk_arrays
    size_t cache_count_ = 0; // number of chunk arrays in the cache.
    size_t max_cache_count = 50; //upper bound on number of chunk arrays to cache in memory  
    Array* keys_; //array of keys that list what's in this array
    size_t uid_; //unique identifier for this array - used for chunks
    size_t chunkSize_ = 1024; //number of elements to host in each chunk
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
        chunkArray_ = new DoubleArray(chunkSize_);
    }

    DoubleDistributedArray(char* serialized, KVStore* kv) {
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
        this->kv_ = kv;
        this->chunkArray_ = new DoubleArray(JSONHelper::getValueFromKey("chunkArray_", payload)->c_str());
    }
    /*
    *   Gets the int at a given index
    */ 
    double get(size_t index) {
        assert(index < itemCount_);
        //std::cout << "in get " << index <<"\n";
        if (cache_ == nullptr) {
            init_cache_();
        }
        double res;
        if(index/chunkSize_ == chunkCount_) {
            res = chunkArray_->get(index % chunkSize_);
        } else {

            size_t key_idx = floor((index/chunkSize_));
            //std::cout << "key index: " << key_idx << "\n";
            //std::cout << key
            DoubleArray* doubleVals;
            if (cache_[key_idx] != nullptr) {
                //std::cout << "get chunk from cache\n";
                doubleVals = cache_[key_idx];
            } else {
                //std::cout << "get chunk from kv\n";
                doubleVals = get_chunk_from_kv_(key_idx);
            }
            res = doubleVals->get(index % chunkSize_);
        }
        //std::cout << "res: " << res << "\n";
        return res; //returns int at correct index in array at this chunk
    };

    //make call to KVStore. Put item in cache. increment cache count.
    DoubleArray* get_chunk_from_kv_(size_t key_idx) {
        if (cache_count_ >= max_cache_count) {
            reset_cache_();
        }
        Value* v = kv_->get(dynamic_cast<Key*>(keys_->get(key_idx))); //gets value of given key
        char* serlializedNode = v->data; 
        //std::cout << serlializedNode <<"\n";
        DoubleArray* doubleVals = new DoubleArray(serlializedNode); //turns payload into IntArray object
        cache_[key_idx] = doubleVals;
        cache_count_ += 1;
        return doubleVals;
    }

    //free all the pointers in cache. reset cache_count_ to 0.
    void reset_cache_() {
        //std::cout << "reset cache, cache count: " << cache_count_ << "\n";
        for (size_t i = 0; i < keys_->length(); i++) {
            if (cache_[i] != nullptr) {
                //std::cout << "clearing idx: " << i<<"\n";
                delete cache_[i];
                cache_[i] = nullptr;
            }
        }
        cache_count_ = 0;
    }

    void init_cache_() {
        //assuming a dataframe is immutable, so nu pushbacks after the first get call.
        cache_ = new DoubleArray*[keys_->length()];
        for (size_t i = 0; i < keys_->length(); i++) {
            cache_[i] = nullptr;
        }
        return;
    }

    /*
    * Given an index, updates the value at that index
    */
    double set(size_t index, double val) {
        assert(index <= itemCount_);
        if(index == itemCount_) {
            pushBack(val);
            return val;
        }
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
        kv_->put(curKey, new Value(strVals->serialize(), (size_t)0)); //reserialize IntArray with updated value and using same key - put into kv
        return prevVal;
  }; //sets the object at index to be o, returns former object
    
    /*
    *   Generates a new Key Value pair with a value
    */
    void storeChunk() {
        //std::cout << "total nodes: " << totalNodes_ << "\n";
        //creates a unique keyname based on provided ID - could be modified to randomly pick number?
        StrBuff sb;
        sb.c(uid_);
        sb.c("_dist_double_array_chunk_");
        sb.c(chunkCount_);
        String* s = sb.get();
        Key* k = new Key(s->c_str(), curNode_); //create new key with current node and keyName
        keys_->pushBack(k);
        char* serialized = chunkArray_->serialize();
        //std::cout << serialized << "\n";
        kv_->put(k, new Value(serialized, (size_t)0)); //adds new IntArray to kvStore
        delete chunkArray_;
        chunkArray_ = new DoubleArray(chunkSize_); //creates new Int array to store values for new chunk
        curNode_+=1;
        chunkCount_+=1;
        if(curNode_ == totalNodes_) { //starts cycle of chunks on nodes again
            curNode_ = 0;
        }
    }
    /*
    *   Adds a new int to the back of the Distributed Int Array
    */ 
    void pushBack(double val) {
        //initialize first key
        if(itemCount_ % chunkSize_ == 0 && itemCount_ != 0) { //if current chunk is full..
            storeChunk(); //stores the previous chunk values in the kv_store
            chunkArray_->pushBack(val);
        } else {
            chunkArray_->pushBack(val);
        }
        itemCount_ +=1;
        //std::cout << uid_ << " " <<itemCount_ << "\n";
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
        sb->write("chunkArray_", chunkArray_->serialize(), false);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }//serializes this distributed Array

};