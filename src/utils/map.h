#pragma once
#include "string.h"
#include "../object.h"
#include "array.h"
#include "queue.h"
#include "keyVal.h"

//todo: Map should delete all keys, values.
//authors: eldrid.s@husky.neu.edu and shetty.y@husky.neu.edu
/**
* An object that represents a map to store keys and values.
* Map does not own any objects passed to it.
*/
class Map : public Object {

  public:
    size_t numBuckets_;
    size_t bucketsUsed_;
    size_t items_;
    Array *buckets_;

    /* The constructor*/
    Map() { 
      this->numBuckets_ = 32;
      this->bucketsUsed_ = 0;
      this->items_ = 0;
      this->buckets_ = new Array(32);
      for (size_t i = 0; i < this->numBuckets_; i++) {
        this->buckets_->pushBack(new Queue());
      }
    }

    /* The destructor*/
    virtual ~Map() { 
      delete this->buckets_;
    }

    /**
    * Determines the number of items in the map
    * @return the size of the map
    */
    size_t size() {
      return this->items_;
    }

    /**
    * Adds the given key value pair to the Map
    * @param key is the object to map the value to
    * @param value the object to add to the Map
    */
    void add(Object* key, Object* value) {
      size_t key_hash = key->hash();
      KeyVal *key_val = new KeyVal(key, value);
      int index = key_hash % this->numBuckets_;
      Queue *queue_at_index = dynamic_cast<Queue*>(this->buckets_->get(index));
      if (queue_at_index->size() == 0) {
        this->bucketsUsed_ += 1;
        if (((float) this->bucketsUsed_ / (float) this->numBuckets_) > 0.75) {
          this->rehash_();
        }
      } 
      if (this->get(key) == nullptr) {
        queue_at_index->add(key_val);
      } else {
        Object* removed = this->pop_item(key);
        String *before  = dynamic_cast<String*>(key_val->getVal());
        queue_at_index->add(key_val);
        String *after  = dynamic_cast<String*>(this->get(key));
      }
      this->items_ += 1;
    }

    /**
    * Removes all the elements from the Map
    */
    void clear() {
      this->numBuckets_ = 32;
      this->bucketsUsed_ = 0;
      this->items_ = 0;
      delete this->buckets_;
      this->buckets_ = new Array(32);
      for (size_t i = 0; i < this->numBuckets_; i++) {
        this->buckets_->pushBack(new Queue());
      }
    }

    /**
    * Returns a copy of the Map
    * @return the copy of this map
    */
    Map* clone() {
      Map *new_map = new Map();
      Object** this_keys = this->keys();
      size_t len = this->size();
      for (size_t i = 0; i < len; i++) {
        new_map->add(this_keys[i], this->get(this_keys[i]));
      }
      return new_map;
    }

    /**
    * Returns the value of the specified key
    * @param key the key to get the value from
    * @return the value associated with the key
    */
    Object* get(Object* key) {
      KeyVal *wrappedkey = new KeyVal(key, nullptr);
      size_t key_hash = key->hash();
      int index = key_hash % this->numBuckets_;
      Queue *queue_at_index = dynamic_cast<Queue *>(this->buckets_->get(index));
      if (queue_at_index->get(wrappedkey) != nullptr) {
        //key is in map.
        KeyVal *key_val = dynamic_cast<KeyVal *>(queue_at_index->get(wrappedkey));
        return key_val->getVal();
      } else {
        //key is not in map
        return nullptr;
      }
    }

    /**
    * Returns the Map's keys.
    */
    Object** keys() {
      Object** keys = new Object*[this->items_];
      size_t index_to_assign = 0;
      for (size_t i = 0; i < this->numBuckets_; i++) {
        Queue *queue_at_index = dynamic_cast<Queue*>(this->buckets_->get(i));
        for (size_t j = 0; j < queue_at_index->size(); j++) {
          KeyVal *key_val = dynamic_cast<KeyVal *>(queue_at_index->get(j));
          keys[index_to_assign] = key_val->getKey();
          index_to_assign +=1;
        }
      }
      return keys;
    }

    /**
    * Returns all the Map's values
    */
    Object** values() {
      Object** values = new Object*[this->items_];
      size_t index_to_assign = 0;
      for (size_t i = 0; i < this->numBuckets_; i++) {
        Queue *queue_at_index = dynamic_cast<Queue *>(this->buckets_->get(i));
        for (size_t j = 0; j < queue_at_index->size(); j++) {
          KeyVal *key_val = dynamic_cast<KeyVal *>(queue_at_index->get(j));
          values[index_to_assign] = key_val->getVal();
          index_to_assign +=1;
        }
      }

      return values;
    }

    /**
    * Removes the element with the specified key
    * @param key the key
    * @return the value of the element removed
    */
    Object* pop_item(Object* key) {
      size_t key_hash = key->hash();
      int index = key_hash % this->numBuckets_;
      Queue *queue_at_index = dynamic_cast<Queue *>(this->buckets_->get(index));
      KeyVal *wrappedkey = new KeyVal(key, nullptr);
      this->items_ -= 1;
      if (queue_at_index->size() == 0) {
        this->bucketsUsed_ -= 1;    
      }
      KeyVal *keyVal = dynamic_cast<KeyVal*>(queue_at_index->remove(wrappedkey));
      return keyVal->getVal();
    }

    /**
		* Is this object equal to that object?
		* @param o is the object to compare equality to.
		* @return	whether this object is equal to that object.
		*/
		virtual bool equals(Object* o) {
      if (o == nullptr) return false;
      Map *m = dynamic_cast<Map*>(o);
      if (o == nullptr) {
        return false;
      }
      
      if (this->size() != m->size()) return false;

      Object** this_keys = this->keys();
      for (size_t i = 0; i < this->size(); i++) {
        Object* this_val = this->get(this_keys[i]);
        Object* that_val = m->get(this_keys[i]);

        if (!this_val->equals(that_val)) {
          return false;
        }
      }

      return true;
    }

		/**
		* Calculate this object's hash.
		* @return a natural number of a hash for this object.
		*/
		virtual size_t hash() {
      return this->buckets_->hash(); 
    }

    void rehash_() {
      KeyVal *key_val[this->items_];
      Object** this_keys = this->keys();
      for (size_t i = 0; i < this->size(); i++) {
        key_val[i] = new KeyVal(this_keys[i], this->get(this_keys[i]));
      }

      size_t items = this->items_;

      this->numBuckets_ = 2 * this->numBuckets_;
      this->bucketsUsed_ = 0;
      this->items_ = 0;

      delete this->buckets_;

      this->buckets_ = new Array(this->numBuckets_);

      for (size_t i = 0; i < items; i++) {
        this->add(key_val[i]->getKey(), key_val[i]->getVal());
      }     
    }
};