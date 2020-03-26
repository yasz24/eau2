//lang::CwC
#pragma once
#include "../object.h"
#include "string.h"
#include "listNode.h"
#include <iostream>
#include "../serialize/deserialize.h"
#include "keyVal.h"
//authors: eldrid.s@husky.neu.edu shetty.y@husky.neu.edu
//todo: destructors
/* A queue of Objects, supporting O(1) insertion at the start of the queue and O(1) retrieval / removal from the end
 * of the queue. */
class Queue : public Object {
public:
  ObjectNode *head_;
  ObjectNode *tail_;
  size_t size_;

  Queue() {
    this->head_ = nullptr;
    this->tail_ = nullptr;
    this->size_ = 0;
  }

  Queue(char* serialized) {
    this->head_ = nullptr;
    this->tail_ = nullptr;
    this->size_ = 0;
    Deserializable* ds = new Deserializable();
    char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
    size_t queueLen = std::stoi(JSONHelper::getValueFromKey("size_", payload)->c_str());
    char* vals = JSONHelper::getValueFromKey("queueObjs_", payload)->c_str();
    for(int i = 0; i < queueLen; i++) {
        char* serial = JSONHelper::getArrayValueAt(vals, i)->c_str();
        std::cout<<serial<<"\n";
        Object* o = ds->deserialize(serial);
        add(o);
    }
  }

  ~Queue() {
  }

  /* Adds the given Object to the start of this Queue */
  void add(Object* o) {
    //create the linked list node.
    ObjectNode *newNode = new ObjectNode(o);
    if (this->head_ == nullptr) {
      //adding in first element. set both head and tail to be that element.
      this->head_ = newNode;
      this->tail_ = newNode;
      //increment size.
      this->size_+=1;
    } else{
      this->tail_->setNext(newNode);
      newNode->setPrev(this->tail_);
      this->tail_ = newNode;
      this->size_+=1;
    }
  }

  /* Adds the given ObjectQueue to the start of this ObjectQueue */
  void add_all(Queue* o) {
    //set the given queue's tail's next to this queue's head.
    if (o->size() > 0) {
      unsigned int len = o->size();
      //go through the given list.
      ObjectNode *curNode = o->head_;
      for (size_t i = 0; i < len; i++) {
        //create new object node with the current nodes data.
        Object *data = curNode->data_;
        //update cur node
        curNode = curNode->getNext();
        //add cur node to this list.
        this->add(data);
      }
    }
  }

  /* Returns if given Object is the same as this ObjectQueue */
  bool equals(Object* o) {
    //if the given object is a nullptr, it's not equal to this.
    if (o == nullptr) return false;
    //if the given pointer is not an ObjectQueue, it's not equal to this.
		Queue *other = dynamic_cast<Queue*>(o);
    //if two queue's don't have the same size they're not equal.
    if (this->size_ != other->size()) {
      return false;
    }

    if (this->size_ > 0) {
      ObjectNode *node1 = this->head_;
      ObjectNode *node2 = other->head_;
      for (size_t i = 0; i < this->size_; i++) {
        //if the node's at the corresponding indices are not the same, the queue's are not equal. 
        if (!(node1->equals(node2))) {
          return false;
        }
        //shift the respective node pointers to the next elements.
        node1 = node1->getNext();
        node2 = node2->getNext();
      }
      return true;

    } else {
      //queue's of size 0 are equal.
      return true;
    } 
  }

  size_t hash() {
    size_t res = 0;
    if (this->size() > 0) {
      unsigned int len = this->size();
      //go through the given list.
      ObjectNode *curNode = this->head_;
      for (size_t i = 0; i < len; i++) {
        Object *data = curNode->data_;
        //add the data's hash val to result.
        res += data->hash();
      }
    }
    return res;
  }


  /* Removes and returns the Object at the end of this ObjectQueue */
  Object* remove() {
    ObjectNode *to_return = this->head_;
    if (this->head_->getNext() != nullptr) {
      //set the heads next's prev to nullptr.
      this->head_->getNext()->setPrev(nullptr);
      //set the head to the second item in queue.
      this->head_ = this->head_->getNext();
      //remove link from first item to second item.
      to_return->setNext(nullptr);
    } else {
      //both head and tail should be set to nullptr
      this->head_ = nullptr;
      this->tail_ = nullptr;
    }
    //decrement size by one
    this->size_ -= 1;

    return to_return->data_;
  }

  /* Clears this ObjectQueue of all elements */
  void clear() {
    this->head_ = nullptr;
    this->tail_ = nullptr;
    this->size_ = 0;
  }

  /* Returns a reference to the Object at the end of this list */
  Object* peek() {
    return this->head_->data_; 
  }
  /* Removes the first instance of a given Object from the queue and returns the removed Object */
  Object* remove(Object* o) {
    if (this->size() > 0) {
      unsigned int len = this->size();
      ObjectNode *to_remove = nullptr;
      //go through the given list.
      ObjectNode *curNode = this->head_;
      for (size_t i = 0; i < len; i++) {
        //check if curNode's data is the same as o.
        if (curNode->data_->equals(o)) {
          //set the first node that is equal to the object to be removed.
          to_remove = curNode;
          break;
        }
        curNode = curNode->getNext(); 
      }
      if (!(to_remove == nullptr)) {
        if (to_remove->equals(this->head_)) {
          this->head_ = this->head_->getNext();
        }
        if (to_remove->equals(this->tail_)) {
          this->tail_ = this->tail_->getPrev();
        }
        //set to_remove's next's prev to to_remove's prev
        if (to_remove->getNext() != nullptr) {
          to_remove->getNext()->setPrev(to_remove->getPrev());
        }
        if (to_remove->getPrev() != nullptr) {
          to_remove->getPrev()->setNext(to_remove->getNext());
        }
        to_remove->setNext(nullptr);
        to_remove->setPrev(nullptr);
        this->size_ -= 1;
        return to_remove->data_;
      }
    }
    return nullptr;
  }

  /* Returns a given object if that Object can be found in the queue */
  Object *get(Object* o) {
    if (this->size() > 0) {
      unsigned int len = this->size();
      //go through the given list.
      ObjectNode *curNode = this->head_;
      for (size_t i = 0; i < len; i++) {
        if (curNode->data_->equals(o)) {
          return curNode->data_;
        }
        curNode = curNode ->getNext();
      }
    }
    return nullptr;
  }

  /* Returns a given object at the provided index */
  Object *get(int to_get) {
    if (this->size() > 0) {
      unsigned int len = this->size();
      //go through the given list.
      ObjectNode *curNode = this->head_;
      for (size_t i = 0; i < len; i++) {
        if (to_get == i) {
          return curNode->data_;
        }
        curNode = curNode ->getNext();
      }
    }
    return nullptr;
  }

  /* Returns the number of elements in this ObjectQueue */
  size_t size() {
    return this->size_;
  }

  char* serialize() {
    Serializable* sb = new Serializable();
    sb->initSerialize("Queue");
    sb->write("size_", size_);
    Object** objs_ = new Object*[this->size_];
    //go through the given list.
    for (size_t i = 0; i < this->size_; i++) {
        objs_[i] =  this->get(i);
    }
    sb->write("queueObjs_", objs_, this->size_);
    sb->endSerialize();
    char* value = sb->get();
    delete sb;
    return value;
  }
};