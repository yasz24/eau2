//lang: CwC
#pragma once
#include "object.h"
#include "../utils/string.h"
#include "../serialize/jsonHelper.h"
#include "../serialize/serial.h"
#include <string>
//Authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu
/** Array class: creates a resizeable array of Objects */

/**Includes unique serialization and deserialization methods for IntArray, FloatArray, and StringArray 
 * that allow these classes to be interpreted as JSON strings*/
class Array: public Object{
public:
    Object** objs_; //array of objects
    size_t listLength_;
    size_t arraySize_;

    Array() {
        objs_ = new Object*[5];
        arraySize_ = 5;
        listLength_ = 0;
    }; // Creates an array of size length, where each object is Object* long

    Array(size_t length) {
        objs_ = new Object*[length];
        arraySize_ = length;
        listLength_ = 0;
    }; //allows you to create an array with a specific start size

  virtual ~Array() {
      delete [] objs_;
  };

  void extend_array(int newLength) {
        if(arraySize_ <= newLength) {
            int oldSize = arraySize_;
            arraySize_ = newLength*2;
            Object** newObjs = new Object*[arraySize_];
            for (size_t i = 0; i < oldSize ; i++) {
                newObjs[i] = objs_[i];
            }
            delete [] objs_;
            objs_ = newObjs;
        }
        hash_ = 0; //every time the array is
    }//necessary to extend array size that stores strings

  virtual Object* get(size_t index) {
      if(listLength_ == 0 || index > listLength_-1) {
            return nullptr;
        }
        return objs_[index];
  }; //returns the object at index

  virtual Object* set(size_t index, Object* o) {
      if(index > listLength_-1) {
            return nullptr;
        }
        Object* temp = get(index);
        objs_[index] = o;
        if(temp == nullptr) {
            listLength_++;
            return nullptr;
        }
        hash_ = 0; //since list has changed in composition, reset stored hash
        return temp;
  }; //sets the object at index to be o, returns former object

  virtual Object* remove(size_t index) {
      if(index > listLength_-1) {
            return nullptr;
        }
        Object* temp = get(index);
        for(int i = index; i<listLength_; i++) {
            if(i < listLength_-1) {
                objs_[i] = objs_[i+1];
            }
        }
        listLength_--;
        hash_ = 0; //since list has changed in composition, reset stored hash
        return temp;
  }; //Replaces object at index with nullptr, returns object
    
    virtual void pushBack(Object* o) {
        int newSize = 1+listLength_;
        extend_array(newSize);
        objs_[listLength_] = o;
        listLength_++;
    }; //add o to end of array

  bool empty() {
      return listLength_ == 0;
  }; //checks if there are any items in the array

  size_t hash() {
       size_t res = 0;
        for(int i = 0; i < listLength_; i++) {
            //specific code to deal with the union data-type
            res+=objs_[i]->hash();
        }
        return res;
  }; //returns the hash of the array

  bool equals(Array* o) {
      if ( o == nullptr) return false;
        Array *s = dynamic_cast<Array*>(o);
        if(s == nullptr) return false;
        if(listLength_ != s->listLength_) return false;
        for(int i = 0; i < listLength_; i++) {
            Object * other = s->objs_[i];
            bool sameString = objs_[i]->equals(other);
            if(!sameString) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  void clear() {
    arraySize_ = 5; 
    listLength_ = 0;
    Object** newObjs = new Object*[arraySize_];
    delete [] objs_;
    objs_ = newObjs;
  }; // creates new array for data storage and dumps old one

  size_t length() {
      return listLength_;
  };
    /** Return a copy of the object; nullptr is considered an error */
    virtual Object* clone() { return nullptr; }

    /** Compute the hash code (subclass responsibility) */
    virtual size_t hash_me() { return 1; };

    /** Returned c_str is owned by the object, don't modify nor delete. */
    virtual char* c_str() { return nullptr; }
};
/** Builds a specific type of array with similar behavior to Array class but of fixed length and Ints */
class IntArray: public Object {
    public:
    int* vals_;
    size_t listLength_ = 0;
    size_t arraySize_;

    IntArray() {
        arraySize_ = 32;
        vals_=new int[arraySize_];
    }

    IntArray(int as) {
        arraySize_ = as;
        vals_=new int[arraySize_];
    }

   int get(size_t index) {
      assert(index < listLength_);
        return vals_[index];
    }; //returns the object at index

  int set(size_t index, int val) {
     assert(index < listLength_);
    int prev = vals_[index];
    vals_[index] = val;
    return prev;
  }; //sets the object at index to be o, returns former object
    
    void pushBack(int val) {
        vals_[listLength_] = val;
        listLength_++;
    }; //add o to end of array

  bool empty() {
      return listLength_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        IntArray *s = dynamic_cast<IntArray*>(o);
        if(s == nullptr) return false;
        if(listLength_ != s->listLength_) return false;
        for(int i = 0; i < listLength_; i++) {
            bool sameInt = s->get(i) == get(i);
            if(!sameInt) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return listLength_;
  }; //returns the number of elements in the array

  /** Return the hash value of this object */
  size_t hash_me() {
      int temp = 0;
      for(int i = 0; i < listLength_; i++) {
          temp *= vals_[i];
      }
      return temp;
  }

    /** Return a copy of the object; nullptr is considered an error */
    virtual Object* clone() { 
        IntArray* temp = new IntArray();
        for(int i = 0; i < listLength_; i++) {
            int value = vals_[i];
            temp->pushBack(value);
        }
        return temp;
     }
     /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */    
     char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("IntArray");
        sb->write("listLength_", listLength_);
        sb->write("arraySize_", arraySize_);
        sb->write("vals_", vals_, listLength_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
     }
    /**
     * Method of deserialization that creates a new instance of this class with all the same data as the provided serialized object
     */ 
     static IntArray* deserialize(char* s) {
         size_t arraySize = std::stoi(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
         size_t listLength = std::stoi(JSONHelper::getValueFromKey("listLength_", s)->c_str());
         String* vals = JSONHelper::getValueFromKey("vals_", s);
         char* values = vals->c_str();
         IntArray* ia = new IntArray(arraySize);
         for(int i = 0; i < listLength; i++) {
             ia->pushBack(std::stoi(JSONHelper::getArrayValueAt(values, i)->c_str()));
         }
         return ia;
     }
};
/** Builds a specific type of array with similar behavior to Array class but of fixed length and doubles */
class DoubleArray: public Object {
    public:
    double* vals_;
    size_t listLength_ = 0;
    size_t arraySize_;

    DoubleArray() {
        arraySize_ = 32;
        vals_=new double[arraySize_];
    }

    DoubleArray(int as) {
        arraySize_ = as;
        vals_=new double[arraySize_];
    }

    double get(size_t index) {
        assert(index < listLength_);
        return vals_[index];
    }; //returns the object at index

  double set(size_t index, double val) {
     assert(index < listLength_);
    double prev = vals_[index];
    vals_[index] = val;
    return prev;
  }; //sets the object at index to be o, returns former object
    
    void pushBack(double val) {
        vals_[listLength_] = val;
        listLength_++;
    }; //add o to end of array

  bool empty() {
      return listLength_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        DoubleArray *s = dynamic_cast<DoubleArray*>(o);
        if(s == nullptr) return false;
        if(listLength_ != s->listLength_) return false;
        for(int i = 0; i < listLength_; i++) {
            bool sameDoub = s->get(i) == get(i);
            if(!sameDoub) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return listLength_;
  }; //returns the number of elements in the array

  /** Return the hash value of this object */
  size_t hash_me() {
      double temp = 0;
      for(int i = 0; i < listLength_; i++) {
          temp *= vals_[i];
      }
      return temp;
  }

    /** Return a copy of the object; nullptr is considered an error */
    virtual Object* clone() { 
        IntArray* temp = new IntArray();
        for(int i = 0; i < listLength_; i++) {
            int value = vals_[i];
            temp->pushBack(value);
        }
        return temp;
     }
     /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */    
     char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("DoubleArray");
        sb->write("listLength_", listLength_);
        sb->write("arraySize_", arraySize_);
        sb->write("vals_", vals_, listLength_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
     }
    /**
     * Method of deserialization that creates a new instance of this class with all the same data as the provided serialized object
     */ 
     static DoubleArray* deserialize(char* s) {
         size_t arraySize = std::stoi(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
         size_t listLength = std::stoi(JSONHelper::getValueFromKey("listLength_", s)->c_str());
         String* vals = JSONHelper::getValueFromKey("vals_", s);
         char* values = vals->c_str();
         DoubleArray* da = new DoubleArray(arraySize);
         for(int i = 0; i < listLength; i++) {
             da->pushBack(std::stoi(JSONHelper::getArrayValueAt(values, i)->c_str()));
         }
         return da;
     }
};
/** Builds a specific type of array with similar behavior to Array class but of fixed length and Floats */
class FloatArray: public Object {
    public:
    float* vals_;
    size_t listLength_ = 0;
    size_t arraySize_;

    FloatArray() {
        arraySize_ = 32;
        vals_ = new float[arraySize_];
    }

    FloatArray(int as) {
        arraySize_ = as;
        vals_ = new float[arraySize_];
    }

   float get(size_t index) {
      assert(index < listLength_);
        return vals_[index];
    }; //returns the object at index

  float set(size_t index, float val) {
     assert(index < listLength_);
    float prev = vals_[index];
    vals_[index] = val;
    return prev;
  }; //sets the object at index to be o, returns former object
    
    void pushBack(float val) {
        vals_[listLength_] = val;
        listLength_++;
    }; //add o to end of array

  bool empty() {
      return listLength_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        FloatArray *f = dynamic_cast<FloatArray*>(o);
        if(f == nullptr) return false;
        if(listLength_ != f->listLength_) return false;
        for(int i = 0; i < listLength_; i++) {
            bool sameFloat = f->get(i) == get(i);
            if(!sameFloat) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return listLength_;
  }; //returns the number of elements in the array

  /** Return the hash value of this object */
  size_t hash_me() {
      float temp = 0;
      for(int i = 0; i < listLength_; i++) {
          temp *= vals_[i];
      }
      return (int)temp;
  }

    /** Return a copy of the object; nullptr is considered an error */
    virtual Object* clone() { 
        FloatArray* temp = new FloatArray();
        for(int i = 0; i < listLength_; i++) {
            float value = vals_[i];
            temp->pushBack(value);
        }
        return temp;
     }
    /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */ 
    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("FloatArray");
        sb->write("listLength_", listLength_);
        sb->write("arraySize_", arraySize_);
        sb->write("vals_", vals_, listLength_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
     }
    /**
     * Method of deserialization that creates a new instance of this class with all the same data as the provided serialized object
     */ 
     static FloatArray* deserialize(char* s) {
         size_t arraySize = std::stof(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
         size_t listLength = std::stof(JSONHelper::getValueFromKey("listLength_", s)->c_str());
         String* vals = JSONHelper::getValueFromKey("vals_", s);
         char* values = vals->c_str();
         FloatArray* ia = new FloatArray(arraySize);
         for(int i = 0; i < listLength; i++) {
             ia->pushBack(std::stof(JSONHelper::getArrayValueAt(values, i)->c_str()));
         }
         return ia;
     }
};
/** Builds a specific type of array with similar behavior to Array class but of fixed length and Bools */
class BoolArray: public Object {
    public:
    bool* vals_;
    size_t listLength_ = 0;
    size_t arraySize_;

    BoolArray() {
        arraySize_ = 32;
        vals_ = new bool[arraySize_];
    }

    BoolArray(int as) {
        arraySize_ = as;
        vals_ = new bool[arraySize_];
    }

   bool get(size_t index) {
      assert(index < listLength_);
        return vals_[index];
    }; //returns the object at index

  bool set(size_t index, bool val) {
     assert(index < listLength_);
    bool prev = vals_[index];
    vals_[index] = val;
    return prev;
  }; //sets the object at index to be o, returns former object
    
    void pushBack(bool val) {
        vals_[listLength_] = val;
        listLength_++;
    }; //add o to end of array

  bool empty() {
      return listLength_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        BoolArray *b = dynamic_cast<BoolArray*>(o);
        if(b == nullptr) return false;
        if(listLength_ != b->listLength_) return false;
        for(int i = 0; i < listLength_; i++) {
            bool sameBool = b->get(i) == get(i);
            if(!sameBool) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return listLength_;
  }; //returns the number of elements in the array

  /** Return the hash value of this object */
  size_t hash_me() {
      return 1;
  }

    /** Return a copy of the object; nullptr is considered an error */
    virtual Object* clone() { 
        FloatArray* temp = new FloatArray();
        for(int i = 0; i < listLength_; i++) {
            float value = vals_[i];
            temp->pushBack(value);
        }
        return temp;
     }
};
/** Builds a specific type of array with similar behavior to Array class but of fixed length and Strings* */
class StringArray: public Object {
    public:
    String** vals_;
    size_t listLength_ = 0;
    size_t arraySize_;

    StringArray() {
        arraySize_ = 32;
        vals_ = new String*[arraySize_];
    }

    StringArray(int as) {
        arraySize_ = as;
        vals_ = new String*[arraySize_];
    }

   String* get(size_t index) {
      assert(index < listLength_);
        return vals_[index];
    }; //returns the object at index

  String* set(size_t index, String* val) {
     assert(index < listLength_);
    String* prev = vals_[index];
    vals_[index] = val;
    return prev;
  }; //sets the object at index to be o, returns former object
    
    void pushBack(String* val) {
        vals_[listLength_] = val;
        listLength_++;
    }; //add o to end of array

  bool empty() {
      return listLength_ == 0;
  }; //checks if there are any items in the array

  bool equals(Object * o) {
      if ( o == nullptr) return false;
        StringArray *b = dynamic_cast<StringArray*>(o);
        if(b == nullptr) return false;
        if(listLength_ != b->listLength_) return false;
        for(int i = 0; i < listLength_; i++) {
            bool sameString = b->get(i)->equals(get(i));
            if(!sameString) {
                return false;
            }
        }
        return true;
  }; //checks if this is equal to o

  size_t length() {
      return listLength_;
  }; //returns the number of elements in the array

  /** Return the hash value of this object */
  size_t hash_me() {
      return 1;
  }
    /**
     * Creates a char* serialized version of this class, storing all necessary fields and variables in a JSON string
     */ 
  char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("StringArray");
        sb->write("listLength_", listLength_);
        sb->write("arraySize_", arraySize_);
        sb->write("vals_", vals_, listLength_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
     }
    /**
     * Method of deserialization that creates a new instance of this class with all the same data as the provided serialized object
     */ 
     static StringArray* deserialize(char* s) {
         size_t arraySize = std::stoi(JSONHelper::getValueFromKey("arraySize_", s)->c_str());
         size_t listLength = std::stoi(JSONHelper::getValueFromKey("listLength_", s)->c_str());
         String* vals = JSONHelper::getValueFromKey("vals_", s);
         char* values = vals->c_str();
         StringArray* ia = new StringArray(arraySize);
         for(int i = 0; i < listLength; i++) {
             ia->pushBack(JSONHelper::getArrayValueAt(values, i));
         }
         return ia;
     }
};