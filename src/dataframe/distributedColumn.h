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


  // DistributedIntColumn(char* serialized) {
  //   char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
  //   this->arraySize_ = std::stoi(JSONHelper::getValueFromKey("arraySize_", payload)->c_str());
  //   this->listLength_ = std::stoi(JSONHelper::getValueFromKey("listLength_", payload)->c_str());
  //   this->val_ = new Array(JSONHelper::getValueFromKey("val_", payload)->c_str());
  // }

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

  // char* serialize() {
  //   Serializable* sb = new Serializable();
  //   sb->initSerialize("DistributedIntColumn");
  //   sb->write("listLength_", listLength_);
  //   sb->write("arraySize_", arraySize_);
  //   sb->write("metaArrayStartSize__", metaArrayStartSize_);
  //   char * seralizedArr = val_->serialize();
  //   sb->write("val_", seralizedArr, false);
  //   sb->endSerialize();
  //   char* value = sb->get();
  //   delete sb;
  //   return value;
  // }
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

  // char* serialize() {
  //   Serializable* sb = new Serializable();
  //   sb->initSerialize("DistributedDoubleColumn");
  //   sb->write("listLength_", listLength_);
  //   sb->write("arraySize_", arraySize_);
  //   sb->write("metaArrayStartSize__", metaArrayStartSize_);
  //   char * seralizedArr = val_->serialize();
  //   sb->write("val_", seralizedArr, false);
  //   sb->endSerialize();
  //   char* value = sb->get();
  //   delete sb;
  //   return value;
  // }

  // static DistributedDoubleColumn* deserialize(char* s) {
  //   size_t arraySize = std::stoi(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
  //   size_t listLength = std::stoi(JSONHelper::getValueFromKey("listLength_", s)->c_str());
  //   String* arr_string = JSONHelper::getValueFromKey("val_", s);
  //   char* arr_cstr = arr_string->c_str();
  //   Array* arr = new Array(arr_cstr);
  //   DistributedDoubleColumn* dc = new DistributedDoubleColumn(arr, listLength);
  //   return dc;
  // }
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

  // char* serialize() {
  //   Serializable* sb = new Serializable();
  //   sb->initSerialize("DistributedFloatColumn");
  //   sb->write("listLength_", listLength_);
  //   sb->write("arraySize_", arraySize_);
  //   sb->write("metaArrayStartSize__", metaArrayStartSize_);
  //   char * seralizedArr = val_->serialize();
  //   sb->write("val_", seralizedArr, false);
  //   sb->endSerialize();
  //   char* value = sb->get();
  //   delete sb;
  //   return value;
  // }

  // static DistributedFloatColumn* deserialize(char* s) {
  //   size_t arraySize = std::stoi(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
  //   size_t listLength = std::stoi(JSONHelper::getValueFromKey("listLength_", s)->c_str());
  //   String* arr_string = JSONHelper::getValueFromKey("val_", s);
  //   char* arr_cstr = arr_string->c_str();
  //   Array* arr = new Array(arr_cstr);
  //   DistributedFloatColumn* fc = new DistributedFloatColumn(arr, listLength);
  //   return fc;
  // }
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

  // char* serialize() {
  //   Serializable* sb = new Serializable();
  //   sb->initSerialize("DistributedBoolColumn");
  //   sb->write("listLength_", listLength_);
  //   sb->write("arraySize_", arraySize_);
  //   sb->write("metaArrayStartSize__", metaArrayStartSize_);
  //   char * seralizedArr = val_->serialize();
  //   sb->write("val_", seralizedArr, false);
  //   sb->endSerialize();
  //   char* value = sb->get();
  //   delete sb;
  //   return value;
  // }

  // static DistributedBoolColumn* deserialize(char* s) {
  //   size_t arraySize = std::stoi(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
  //   size_t listLength = std::stoi(JSONHelper::getValueFromKey("listLength_", s)->c_str());
  //   String* arr_string = JSONHelper::getValueFromKey("val_", s);
  //   char* arr_cstr = arr_string->c_str();
  //   Array* arr = new Array(arr_cstr);
  //   DistributedBoolColumn* bc = new DistributedBoolColumn(arr, listLength);
  //   return bc;
  // }
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

  // DistributedStringColumn(char* serialized) {
  //   char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
  //   this->arraySize_ = std::stoi(JSONHelper::getValueFromKey("arraySize_", payload)->c_str());
  //   this->listLength_ = std::stoi(JSONHelper::getValueFromKey("listLength_", payload)->c_str());
  //   this->val_ = new Array(JSONHelper::getValueFromKey("val_", payload)->c_str());
  // }

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

  // char* serialize() {
  //   Serializable* sb = new Serializable();
  //   sb->initSerialize("DistributedStringColumn");
  //   sb->write("listLength_", listLength_);
  //   sb->write("arraySize_", arraySize_);
  //   sb->write("metaArrayStartSize__", metaArrayStartSize_);
  //   char * seralizedArr = val_->serialize();
  //   sb->write("val_", seralizedArr, false);
  //   sb->endSerialize();
  //   char* value = sb->get();
  //   delete sb;
  //   return value;
  // }
  // static DistributedStringColumn* deserialize(char* s) {
  //   size_t arraySize = std::stoi(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
  //   size_t listLength = std::stoi(JSONHelper::getValueFromKey("listLength_", s)->c_str());
  //   String* arr_string = JSONHelper::getValueFromKey("val_", s);
  //   char* arr_cstr = arr_string->c_str();
  //   Array* arr = new Array(arr_cstr);
  //   DistributedStringColumn* sc = new DistributedStringColumn(arr, listLength);
  //   return sc;
  // }
};