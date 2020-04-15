//lang: CwC
#pragma once
#include "../utils/string.h"
#include "../utils/array.h"
#include "../utils/distributedArray.h"
#include "../store/kvstore.h"
#include "../serialize/deserialize.h"
#include "../serialize/jsonHelper.h"
#include "column.h"
#include<stdarg.h>
#include<stdio.h>

//Authors: Shetty.y@husky.neu.edu eldrid.s@husky.neu.edu

/*************************************************************************
 * DistributedIntColumn::
 * Holds int values.
 */
class DistributedIntColumn : public Column {
 public:
  KVStore* kv_;
  IntDistributedArray* val_;

  DistributedIntColumn(KVStore* kv) {
      this->kv_ = kv;
      this->val_ = new IntDistributedArray(kv);
  };


  DistributedIntColumn(char* serialized, KVStore* kv) {
    kv_ = kv;
    char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
    this->val_ = new IntDistributedArray(JSONHelper::getValueFromKey("val_", payload)->c_str(), kv);
  }

  ~DistributedIntColumn() {
   // delete val_;
  }

  IntColumn* as_int() { return nullptr; }
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return nullptr; };
  DoubleColumn* as_double() { return nullptr; };
  DistributedIntColumn * as_dist_int() { 
    return this;
  };
  DistributedDoubleColumn * as_dist_double() { return nullptr; };
  DistributedBoolColumn*  as_dist_bool() { return nullptr; };
  DistributedFloatColumn* as_dist_float() { return nullptr; };
  DistributedStringColumn* as_dist_string() { return nullptr; };

  void storeChunks() { 
    this->val_->storeChunk();
  };

  /** get int value at idx. An out of bound idx triggers an assert error.  */
  int get(size_t idx) {
    return this->val_->get(idx);
  };

  /** Set value at idx. An out of bound idx triggers an assert error.  */
  void set(size_t idx, int val) {
    this->val_->set(idx, val);
  };
  /** returns length of int column */
  size_t size() { return this->val_->length(); };
 
  /** Type appropriate push_back methods. Calling the wrong method results
    * in no data change. **/
  void push_back(int val) {
    this->val_->pushBack(val);
  };
  void push_back(double val) {};
  void push_back(bool val) {};
  void push_back(float val) {};
  void push_back(String* val) {};
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'I'; };

  /** returns a hash generated by summing all the elements together */
  size_t hash_me() {
    return this->val_->hash_me();
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    DistributedIntColumn* other_ic = dynamic_cast<DistributedIntColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    IntDistributedArray* other_val_ = dynamic_cast<IntDistributedArray*>(other_ic->val_);
    return this->val_->equals(other_val_);
  }

  char* serialize() {
    Serializable sb;
    sb.initSerialize("DistributedIntColumn");
    char * seralizedArr = val_->serialize();
    sb.write("val_", seralizedArr, false);
    sb.endSerialize();
    char* value = sb.get();
    return value;
  }

  /**
   * Build and return a non-distributed Column containing data from chunks that live in the local kv_store.
  */
  Column* getColumnOnNode() override {
    size_t _node = kv_->this_node_;
    IntColumn* col = new IntColumn();
    Array* keys = val_->keys_;
    for (size_t i = 0; i < keys->length(); i++) {
      Key* key = dynamic_cast<Key*>(val_->keys_->get(i));
      if (key->node_ == _node) { //only get the chunks that live locally on this node.
        Value* val = kv_->get(key);
        Deserializable ds;
        IntArray* chunk = dynamic_cast<IntArray*>(ds.deserialize(val->data));
        for (size_t j = 0; j < chunk->length(); j++) {
          col->push_back(chunk->get(j));
        }
        delete chunk;
      }
    }
    //if node 0, append the chunk_array_ at the end too.
    if (_node == 0) {
      IntArray* chunk_array = val_->chunkArray_;
      for (size_t j = 0; j < chunk_array->length(); j++) {
          col->push_back(chunk_array->get(j));
      }
    }
    return col;
  }
};
/*************************************************************************
 * DistributedDoubleColumn::
 * Holds double values.
 */
class DistributedDoubleColumn : public Column {
 public:
  KVStore* kv_;
  DoubleDistributedArray* val_;

  DistributedDoubleColumn(KVStore* kv) {
    this->kv_ = kv;
    this->val_ = new DoubleDistributedArray(kv);
  };

  DistributedDoubleColumn(char* serialized, KVStore* kv) {
    kv_ = kv;
    char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
    this->val_ = new DoubleDistributedArray(JSONHelper::getValueFromKey("val_", payload)->c_str(), kv);
  }

  ~DistributedDoubleColumn() {
   // delete val_;
  }

  IntColumn* as_int() { return nullptr; }
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return nullptr; };
  DoubleColumn* as_double() { return nullptr; };
  DistributedIntColumn * as_dist_int() { return nullptr; };
  DistributedDoubleColumn * as_dist_double() { 
    return this; 
  };
  DistributedBoolColumn*  as_dist_bool() { return nullptr; };
  DistributedFloatColumn* as_dist_float() { return nullptr; };
  DistributedStringColumn* as_dist_string() { return nullptr; };
  void storeChunks() { 
    this->val_->storeChunk();
  };
  /** get double value at idx. An out of bound idx triggers an assert error.  */
  double get(size_t idx) {
    return this->val_->get(idx);
  };
  /** Set value at idx. An out of bound idx triggers an assert error.  */
  void set(size_t idx, double val) {
    this->val_->set(idx, val);
  };
  /** returns length of int column */
  size_t size() { return this->val_->length(); };
 
  /** Type appropriate push_back methods. Calling the wrong method results
    * in no data change. **/
  void push_back(double val) {
    this->val_->pushBack(val);
  };
  void push_back(int val) {};
  void push_back(bool val) {};
  void push_back(float val) {};
  void push_back(String* val) {};
 
  /** Return the type of this column as a char: 'S', 'B', 'I', 'D' and 'F'.*/
  char get_type() { return 'D'; };

  /** returns a hash generated by summing all the elements together */
  size_t hash_me() {
    return this->val_->hash_me();
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    DistributedDoubleColumn* other_ic = dynamic_cast<DistributedDoubleColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    DoubleDistributedArray* other_val_ = dynamic_cast<DoubleDistributedArray*>(other_ic->val_);
    return this->val_->equals(other_val_);
  }

  char* serialize() {
    Serializable* sb = new Serializable();
    sb->initSerialize("DistributedDoubleColumn");
    char * seralizedArr = val_->serialize();
    sb->write("val_", seralizedArr, false);
    sb->endSerialize();
    char* value = sb->get();
    delete sb;
    return value;
  }

  /**
   * Build and return a non-distributed Column containing data from chunks that live in the local kv_store.
  */
  Column* getColumnOnNode() override {
    size_t _node = kv_->this_node_;
    DoubleColumn* col = new DoubleColumn();
    Array* keys = val_->keys_;
    for (size_t i = 0; i < keys->length(); i++) {
      Key* key = dynamic_cast<Key*>(val_->keys_->get(i));
      if (key->node_ == _node) { //only get the chunks that live locally on this node.
        Value* val = kv_->get(key);
        Deserializable ds;
        DoubleArray* chunk = dynamic_cast<DoubleArray*>(ds.deserialize(val->data));
        for (size_t j = 0; j < chunk->length(); j++) {
          col->push_back(chunk->get(j));
        }
        delete chunk;
      }
    }
    //if node 0, append the chunk_array_ at the end too.
    if (_node == 0) {
      DoubleArray* chunk_array = val_->chunkArray_;
      for (size_t j = 0; j < chunk_array->length(); j++) {
          col->push_back(chunk_array->get(j));
      }
    }
    return col;
  }
};
/*************************************************************************
 * DistributedFloatColumn::
 * Holds float values.
 */
class DistributedFloatColumn : public Column {
 public:
  KVStore* kv_;
  FloatDistributedArray* val_;

  DistributedFloatColumn(KVStore* kv) {
    this->kv_ = kv;
    this->val_ = new FloatDistributedArray(kv);
  };

  DistributedFloatColumn(char* serialized, KVStore* kv) {
    kv_ = kv;
    char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
    this->val_ = new FloatDistributedArray(JSONHelper::getValueFromKey("val_", payload)->c_str(), kv);
  }

  ~DistributedFloatColumn() {
   // delete val_;
  }

  IntColumn* as_int() { return nullptr; }
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return nullptr; };
  DoubleColumn* as_double() { return nullptr; };
  DistributedIntColumn * as_dist_int() { return nullptr; };
  DistributedDoubleColumn * as_dist_double() { return nullptr; };
  DistributedBoolColumn*  as_dist_bool() { return nullptr; };
  DistributedFloatColumn* as_dist_float() { 
    return this; 
  };
  DistributedStringColumn* as_dist_string() { return nullptr; };
  void storeChunks() { 
    this->val_->storeChunk();
  };

  /** get value at idx. An out of bound idx triggers an assert error.  */
  float get(size_t idx) {
    return this->val_->get(idx);
  };

  /** Set value at idx. An out of bound idx triggers an assert error.  */
  void set(size_t idx, float val) {
   this->val_->set(idx, val);
  };
  /** returns length of column */
  size_t size() { return this->val_->length(); };

 
  /** Type appropriate push_back methods. Calling the wrong method results 
    * in no data change. **/
  void push_back(float val) {
    this->val_->pushBack(val);
  };
  void push_back(double val) {};
  void push_back(bool val) {};
  void push_back(int val) {};
  void push_back(String* val) {};
 
/** returns a hash created by summing int representations of the float values */
  size_t hash_me() {
   return this->val_->hash_me();
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    DistributedFloatColumn* other_ic = dynamic_cast<DistributedFloatColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    FloatDistributedArray* other_val_ = dynamic_cast<FloatDistributedArray*>(other_ic->val_);
    return this->val_->equals(other_val_);
  }

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'F'; };
  
  char* serialize() {
    Serializable* sb = new Serializable();
    sb->initSerialize("DistributedFloatColumn");
    char * seralizedArr = val_->serialize();
    sb->write("val_", seralizedArr, false);
    sb->endSerialize();
    char* value = sb->get();
    delete sb;
    return value;
  }

  /**
   * Build and return a non-distributed Column containing data from chunks that live in the local kv_store.
  */
  Column* getColumnOnNode() override {
    size_t _node = kv_->this_node_;
    FloatColumn* col = new FloatColumn();
    Array* keys = val_->keys_;
    for (size_t i = 0; i < keys->length(); i++) {
      Key* key = dynamic_cast<Key*>(val_->keys_->get(i));
      if (key->node_ == _node) { //only get the chunks that live locally on this node.
        Value* val = kv_->get(key);
        Deserializable ds;
        FloatArray* chunk = dynamic_cast<FloatArray*>(ds.deserialize(val->data));
        for (size_t j = 0; j < chunk->length(); j++) {
          col->push_back(chunk->get(j));
        }
        delete chunk;
      }
    }
    //if node 0, append the chunk_array_ at the end too.
    if (_node == 0) {
      FloatArray* chunk_array = val_->chunkArray_;
      for (size_t j = 0; j < chunk_array->length(); j++) {
          col->push_back(chunk_array->get(j));
      }
    }
    return col;
  }
};
/*************************************************************************
 * DistributedBoolColumn::
 * Holds bool values.
 */
class DistributedBoolColumn : public Column {
 public:
  KVStore* kv_;
  BoolDistributedArray* val_;

  DistributedBoolColumn(KVStore* kv) {
    this->kv_ = kv;
    this->val_ = new BoolDistributedArray(kv);
  };

  DistributedBoolColumn(char* serialized, KVStore* kv) {
    kv_ = kv;
    char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
    this->val_ = new BoolDistributedArray(JSONHelper::getValueFromKey("val_", payload)->c_str(), kv);
  }

  ~DistributedBoolColumn() {
   // delete val_;  
  }

  IntColumn* as_int() { return nullptr; }
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return nullptr; };
  DoubleColumn* as_double() { return nullptr; };
  DistributedIntColumn * as_dist_int() { return nullptr; };
  DistributedDoubleColumn * as_dist_double() { return nullptr; };
  DistributedBoolColumn*  as_dist_bool() { 
    return this; 
  };
  DistributedFloatColumn* as_dist_float() { return nullptr; };
  DistributedStringColumn* as_dist_string() { return nullptr; };
  void storeChunks() { 
    this->val_->storeChunk();
  };

  /** get value at idx. An out of bound idx triggers an assert error.  */
  bool get(size_t idx) {
    return this->val_->get(idx);
  };

  /** Set value at idx. An out of bound idx triggers an assert error. */
  void set(size_t idx, bool val) {
    this->val_->set(idx, val);
  };
  /** returns the length of DistributedBoolColumn */
  size_t size() { return this->val_->length(); };
 
  /** Type appropriate push_back methods. Calling the wrong method results in
    * no data change. **/
  void push_back(bool val) {
    this->val_->pushBack(val);
  };
  void push_back(double val) {};
  void push_back(float val) {};
  void push_back(int val) {};
  void push_back(String* val) {};
 /** returns a hash of the values created by adding position and value of bool to hash */
  size_t hash_me() {
    return this->val_->hash_me();
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    DistributedBoolColumn* other_ic = dynamic_cast<DistributedBoolColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    BoolDistributedArray* other_val_ = dynamic_cast<BoolDistributedArray*>(other_ic->val_);
    return this->val_->equals(other_val_);
  }

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'B'; };

  char* serialize() {
    Serializable* sb = new Serializable();
    sb->initSerialize("DistributedBoolColumn");
    char * seralizedArr = val_->serialize();
    sb->write("val_", seralizedArr, false);
    sb->endSerialize();
    char* value = sb->get();
    delete sb;
    return value;
  }

  /**
   * Build and return a non-distributed Column containing data from chunks that live in the local kv_store.
  */
  Column* getColumnOnNode() override {
    size_t _node = kv_->this_node_;
    BoolColumn* col = new BoolColumn();
    Array* keys = val_->keys_;
    for (size_t i = 0; i < keys->length(); i++) {
      Key* key = dynamic_cast<Key*>(val_->keys_->get(i));
      if (key->node_ == _node) { //only get the chunks that live locally on this node.
        Value* val = kv_->get(key);
        Deserializable ds;
        BoolArray* chunk = dynamic_cast<BoolArray*>(ds.deserialize(val->data));
        for (size_t j = 0; j < chunk->length(); j++) {
          col->push_back(chunk->get(j));
        }
        delete chunk;
      }
    }
    //if node 0, append the chunk_array_ at the end too.
    if (_node == 0) {
      BoolArray* chunk_array = val_->chunkArray_;
      for (size_t j = 0; j < chunk_array->length(); j++) {
          col->push_back(chunk_array->get(j));
      }
    }
    return col;
  }
};
/*************************************************************************
 * DistributedStringColumn::
 * Holds bool values.
 */
class DistributedStringColumn : public Column {
 public:
  KVStore* kv_;
  StringDistributedArray* val_;

  DistributedStringColumn(KVStore* kv) {
    this->kv_ = kv;
    this->val_ = new StringDistributedArray(kv);

  };

  DistributedStringColumn(char* serialized, KVStore* kv) {
    kv_ = kv;
    char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
    this->val_ = new StringDistributedArray(JSONHelper::getValueFromKey("val_", payload)->c_str(), kv);
  }


  ~DistributedStringColumn() {
   // delete val_;
  }

  IntColumn* as_int() { return nullptr; }
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return nullptr; };
  DoubleColumn* as_double() { return nullptr; };
  DistributedIntColumn * as_dist_int() { return nullptr; };
  DistributedDoubleColumn * as_dist_double() { return nullptr; };
  DistributedBoolColumn*  as_dist_bool() { return nullptr; };
  DistributedFloatColumn* as_dist_float() { return nullptr; };
  DistributedStringColumn* as_dist_string() { 
    return this; 
  };
  void storeChunks() { 
    this->val_->storeChunk();
  };

/** Get pointer at idx. An out of bound idx triggers an assert error. */
  String* get(size_t idx) {
    return this->val_->get(idx);
  };
  /** Set pointer at idx. An out of bound idx is undefined.  */
  void set(size_t idx, String* val) {
    this->val_->set(idx, val);
  };
  size_t size() { return this->val_->length();  };
 
  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  void push_back(String* val) {
    this->val_->pushBack(val);
  };
  void push_back(double val) {};
  void push_back(bool val) {};
  void push_back(float val) {};
  void push_back(int val) {};
/** returns a hash generated by summing the hash of all the strings in the column */
  size_t hash_me() {
    return this->val_->hash_me();
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    DistributedStringColumn* other_ic = dynamic_cast<DistributedStringColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    StringDistributedArray* other_val_ = dynamic_cast<StringDistributedArray*>(other_ic->val_);
    return this->val_->equals(other_val_);
  }
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'S'; };

  char* serialize() {
    Serializable* sb = new Serializable();
    sb->initSerialize("DistributedStringColumn");
    char * seralizedArr = val_->serialize();
    sb->write("val_", seralizedArr, false);
    sb->endSerialize();
    char* value = sb->get();
    delete sb;
    return value;
  }

  /**
   * Build and return a non-distributed Column containing data from chunks that live in the local kv_store.
  */
  Column* getColumnOnNode() override {
    size_t _node = kv_->this_node_;
    StringColumn* col = new StringColumn();
    Array* keys = val_->keys_;
    // std::cout << "keys length: " << keys->length() << "\n";
    for (size_t i = 0; i < keys->length(); i++) {
      Key* key = dynamic_cast<Key*>(val_->keys_->get(i));
      if (key->node_ == _node) { //only get the chunks that live locally on this node.
        // std::cout << "here\n";
        Value* val = kv_->get(key);
        Deserializable ds;
        StringArray* chunk = dynamic_cast<StringArray*>(ds.deserialize(val->data));
        for (size_t j = 0; j < chunk->length(); j++) {
          col->push_back(chunk->get(j));
        }
        delete chunk;
      }
    }
    //if node 0, append the chunk_array_ at the end too.
    if (_node == 0) {
      StringArray* chunk_array = val_->chunkArray_;
      for (size_t j = 0; j < chunk_array->length(); j++) {
          col->push_back(chunk_array->get(j));
      }
    }
    return col;
  }
};