//lang: CwC
#pragma once
#include "string.h"
#include "array.h"
#include<stdarg.h>
#include<stdio.h>

//Authors: Shetty.y@husky.neu.edu eldrid.s@husky.neu.edu

 /**************************************************************************
 * Column ::
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable, equality is pointer
 * equality. */
class IntColumn;
class FloatColumn;
class BoolColumn;
class StringColumn;
class Column : public Object {
 public:
 
  /** Type converters: Return same column under its actual type, or
   *  nullptr if of the wrong type.  */
  virtual IntColumn* as_int() = 0;
  virtual BoolColumn*  as_bool() = 0;
  virtual FloatColumn* as_float() = 0;
  virtual StringColumn* as_string() = 0;
 
  /** Type appropriate push_back methods. Calling the wrong method results
    * in no data change. **/
  virtual void push_back(int val) = 0;
  virtual void push_back(bool val) = 0;
  virtual void push_back(float val) = 0;
  virtual void push_back(String* val) = 0;
 
 /** Returns the number of elements in the column. */
  virtual size_t size() = 0;
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  virtual char get_type() = 0;
};
//Column methods were made pure virtual to enforce the abstract nature of Column

/*************************************************************************
 * IntColumn::
 * Holds int values.
 */
class IntColumn : public Column {
 public:
  Array* val_;
  int listLength_ = 0;
  int arraySize_ = 1024;
  int metaArrayStartSize_ = 10;

  IntColumn() {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new IntArray(arraySize_));
    }
  };

  IntColumn(int n, ...) {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new IntArray(arraySize_));
    }
    va_list ap;
    va_start (ap, n);
    for (int i = 0; i < n; i++) {
      push_back(va_arg (ap, int)); 
    }
    va_end (ap);
  }

  ~IntColumn() {
   // delete val_;
  }

  /** get int value at idx. An out of bound idx triggers an assert error.  */
  int get(size_t idx) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
    IntArray* ia = dynamic_cast<IntArray*>(val_->get(arrays_index));
    return ia->get(index);
  };
  /** Set value at idx. An out of bound idx triggers an assert error.  */
  void set(size_t idx, int val) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
    IntArray* ia = dynamic_cast<IntArray*>(val_->get(arrays_index));
    ia->set(index, val);
  };
  /** returns length of int column */
  size_t size() { return listLength_; };
  /** returns this intColumn as an IntColumn */
  IntColumn* as_int() { 
    return this;
   };
  /** returns a nullptr for each type of Column this intColumn is Not */
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return nullptr; };
 
  /** Type appropriate push_back methods. Calling the wrong method results
    * in no data change. **/
  void push_back(int val) {
    if(arraySize_*val_->length() >= listLength_ - 1) {
      val_->pushBack(new IntArray(arraySize_));
    }
    int array_index = listLength_ / arraySize_;
    IntArray* ia = dynamic_cast<IntArray*>(val_->get(array_index));
    ia->pushBack(val);
    listLength_++;

  };
  void push_back(bool val) {};
  void push_back(float val) {};
  void push_back(String* val) {};
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'I'; };

  /** returns a hash generated by summing all the elements together */
  size_t hash_me() {
    size_t hash = 0;
    for(int i = 0; i < val_->length(); i++) {
      IntArray* ia = dynamic_cast<IntArray*>(val_->get(i));
      for(int j = 0; j < ia->length(); j++) {
        hash += ia->get(j);
      }
    }
    return hash;
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    IntColumn* other_ic = dynamic_cast<IntColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    for(int i = 0; i < val_->length(); i++) {
      IntArray* ia = dynamic_cast<IntArray*>(val_->get(i));
      IntArray* other_ia = dynamic_cast<IntArray*>(other_ic->val_->get(i));
      for(int j = 0; j < ia->length(); j++) {
        if(ia->get(j) != other_ia->get(j)) {
          return false;
        }
      }
    }
    return true;
  }
};
/*************************************************************************
 * FloatColumn::
 * Holds float values.
 */
class FloatColumn : public Column {
 public:
  Array* val_;
  int listLength_ = 0;
  int arraySize_ = 1024;
  int metaArrayStartSize_ = 10;

  FloatColumn() {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new FloatArray(arraySize_));
    }
  };

  FloatColumn(int n, ...) {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new FloatArray(arraySize_));
    }
    va_list ap;
    va_start (ap, n);
    for (int i = 0; i < n; i++) {
      push_back(va_arg (ap, float)); 
    }
    va_end (ap);
  }

  ~FloatColumn() {
   // delete val_;
  }

  /** get value at idx. An out of bound idx triggers an assert error.  */
  float get(size_t idx) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
    FloatArray* fa = dynamic_cast<FloatArray*>(val_->get(arrays_index));
    return fa->get(index);
  };

  /** Set value at idx. An out of bound idx triggers an assert error.  */
  void set(size_t idx, float val) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
    FloatArray* fa = dynamic_cast<FloatArray*>(val_->get(arrays_index));
    fa->set(index, val);
  };
  /** returns length of column */
  size_t size() { return listLength_; };

  /** returns this column for the appropriate column, and nullptr for everything else */
  IntColumn* as_int() { return nullptr; };
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return this; };
  StringColumn* as_string() { return nullptr; };
 
  /** Type appropriate push_back methods. Calling the wrong method results 
    * in no data change. **/
  void push_back(float val) {
    if(arraySize_*val_->length() >= listLength_ - 1) {
      val_->pushBack(new FloatArray(arraySize_));
    }
    int array_index = listLength_ / arraySize_;
    FloatArray* fa = dynamic_cast<FloatArray*>(val_->get(array_index));
    fa->pushBack(val);
    listLength_++;

  };
  void push_back(bool val) {};
  void push_back(int val) {};
  void push_back(String* val) {};
 
/** returns a hash created by summing int representations of the float values */
  size_t hash_me() {
    float hash = 0;
    for(int i = 0; i < val_->length(); i++) {
      FloatArray* fa = dynamic_cast<FloatArray*>(val_->get(i));
      for(int j = 0; j < fa->length(); j++) {
        hash += fa->get(j);
      }
    }
    return ((size_t)hash);
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    FloatColumn* other_ic = dynamic_cast<FloatColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    for(int i = 0; i < val_->length(); i++) {
      FloatArray* ia = dynamic_cast<FloatArray*>(val_->get(i));
      FloatArray* other_ia = dynamic_cast<FloatArray*>(other_ic->val_->get(i));
      for(int j = 0; j < ia->length(); j++) {
        if(ia->get(j) != other_ia->get(j)) {
          return false;
        }
      }
    }
    return true;
  }

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'F'; };
};
/*************************************************************************
 * BoolColumn::
 * Holds bool values.
 */
class BoolColumn : public Column {
 public:
  Array* val_;
  int listLength_ = 0;
  int arraySize_ = 1024;
  int metaArrayStartSize_ = 10;

  BoolColumn() {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new BoolArray(arraySize_));
    }
  };

  BoolColumn(int n, ...) {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new BoolArray(arraySize_));
    }
    va_list ap;
    va_start (ap, n);
    for (int i = 0; i < n; i++) {
      push_back(va_arg (ap, bool)); 
    }
    va_end (ap);
  }

  ~BoolColumn() {
   // delete val_;  
  }

  /** get value at idx. An out of bound idx triggers an assert error.  */
  bool get(size_t idx) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
    BoolArray* ba = dynamic_cast<BoolArray*>(val_->get(arrays_index));
    return ba->get(index);
  };

  /** Set value at idx. An out of bound idx triggers an assert error. */
  void set(size_t idx, bool val) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
    BoolArray* ba = dynamic_cast<BoolArray*>(val_->get(arrays_index));
    ba->set(index, val);
  };
  /** returns the length of boolColumn */
  size_t size() { return listLength_; };

  /** returns this for correct type of column, nullptr for everything else */
  IntColumn* as_int() { return nullptr; };
  BoolColumn*  as_bool() { return this; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return nullptr; };
 
  /** Type appropriate push_back methods. Calling the wrong method results in
    * no data change. **/
  void push_back(bool val) {
    if(arraySize_*val_->length() >= listLength_ - 1) {
      val_->pushBack(new BoolArray(arraySize_));
    }
    int array_index = listLength_ / arraySize_;
    BoolArray* ba = dynamic_cast<BoolArray*>(val_->get(array_index));
    ba->pushBack(val);
    listLength_++;

  };
  void push_back(float val) {};
  void push_back(int val) {};
  void push_back(String* val) {};
 /** returns a hash of the values created by adding position and value of bool to hash */
  size_t hash_me() {
    size_t hash = 0;
    for(int i = 0; i < val_->length(); i++) {
      BoolArray* fa = dynamic_cast<BoolArray*>(val_->get(i));
      for(int j = 0; j < fa->length(); j++) {
        int val = 1;
        if(fa->get(j)) {
          val = 0;
        }
        hash += (i + j + val);
      }
    }
    return hash;
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    BoolColumn* other_ic = dynamic_cast<BoolColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    for(int i = 0; i < val_->length(); i++) {
      BoolArray* ia = dynamic_cast<BoolArray*>(val_->get(i));
      BoolArray* other_ia = dynamic_cast<BoolArray*>(other_ic->val_->get(i));
      for(int j = 0; j < ia->length(); j++) {
        if(ia->get(j) != other_ia->get(j)) {
          return false;
        }
      }
    }
    return true;
  }

  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'B'; };
};
/*************************************************************************
 * StringColumn::
 * Holds bool values.
 */
class StringColumn : public Column {
 public:
  Array* val_;
  int listLength_ = 0;
  int arraySize_ = 1024;
  int metaArrayStartSize_ = 10;

  StringColumn() {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new StringArray(arraySize_));
    }
  };

  StringColumn(int n, ...) {
    val_ = new Array();
    for(int i = 0; i < metaArrayStartSize_; i++) {
      val_->pushBack(new StringArray(arraySize_));
    }
    va_list ap;
    va_start (ap, n);
    for (int i = 0; i < n; i++) {
      push_back(va_arg (ap, String*)); 
    }
    va_end (ap);
  }

  ~StringColumn() {
   // delete val_;
  }

/** Get pointer at idx. An out of bound idx triggers an assert error. */
  String* get(size_t idx) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
   StringArray* a = dynamic_cast<StringArray*>(val_->get(arrays_index));
    String* s = dynamic_cast<String*>(a->get(index));
    return s;
  };
  /** Set pointer at idx. An out of bound idx is undefined.  */
  void set(size_t idx, String* val) {
    assert(idx < listLength_);
    int arrays_index = idx / arraySize_;
    int index = idx - arrays_index*arraySize_;
    StringArray* a = dynamic_cast<StringArray*>(val_->get(arrays_index));
    a->set(index, val);
  };
  size_t size() { return listLength_; };
  /** returns this for StringColumn*, nullptr for everything else */
  IntColumn* as_int() { return nullptr; };
  BoolColumn*  as_bool() { return nullptr; };
  FloatColumn* as_float() { return nullptr; };
  StringColumn* as_string() { return this; };
 
  /** Type appropriate push_back methods. Calling the wrong method is
    * undefined behavior. **/
  void push_back(String* val) {
    if(arraySize_*val_->length() >= listLength_ - 1) {
      val_->pushBack(new StringArray(arraySize_));
    }
    int array_index = listLength_ / arraySize_;
    StringArray* a = dynamic_cast<StringArray*>(val_->get(array_index));
    a->pushBack(val);
    listLength_++;

  };
  void push_back(bool val) {};
  void push_back(float val) {};
  void push_back(int val) {};
/** returns a hash generated by summing the hash of all the strings in the column */
  size_t hash_me() {
    size_t hash = 0;
    for(int i = 0; i < val_->length(); i++) {
      StringArray* fa = dynamic_cast<StringArray*>(val_->get(i));
      for(int j = 0; j < fa->length(); j++) {
        hash += fa->get(j)->hash_me();
      }
    }
    return hash;
  }

  bool equals(Object * o) {
    if(o == nullptr) return false;
    StringColumn* other_ic = dynamic_cast<StringColumn*>(o);
    if(other_ic == nullptr) return false;
    if(other_ic->size() != size()) return false;
    for(int i = 0; i < val_->length(); i++) {
      StringArray* ia = dynamic_cast<StringArray*>(val_->get(i));
      StringArray* other_ia = dynamic_cast<StringArray*>(other_ic->val_->get(i));
      for(int j = 0; j < ia->length(); j++) {
        if(!ia->get(j)->equals(other_ia->get(j))) {
          return false;
        }
      }
    }
    return true;
  }
 
  /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'.*/
  char get_type() { return 'S'; };
};