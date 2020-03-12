//lang::CwC
#include "object.h"
#include <iostream>
//authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu
/**
 * ObjectNode: A linked list node that holds an Object.
 * author: shetty.y@husky.neu.edu
*/
class ObjectNode : public Object {
public:
    Object *data_;
    ObjectNode *next_;
    ObjectNode *prev_;

    /**
     * Constructor.
    */
    ObjectNode(Object *data) {
        this->data_ = data;
        this->next_ = nullptr;
        this->prev_ = nullptr;
    }

    ~ObjectNode() {
        
    }

    /**
     * get the next node in the linked list chain.
    */
    virtual ObjectNode *getNext() {
        return this->next_;
    } 


    /**
     * get the prev node in the linked list chain.
    */
    virtual ObjectNode *getPrev() {
        return this->prev_;
    } 

    /**
     * set the next node in the linked list chain.
    */
    virtual ObjectNode *setNext(ObjectNode *next) {
        return this->next_ = next;
    }

     /**
     * set the prev node in the linked list chain.
    */
    virtual ObjectNode *setPrev(ObjectNode *prev) {
        return this->prev_ = prev;
    }


   /**
	 * Overriding the equals from Object class. Tests if the data in this object node is equal to.
	 * @arg other: the Object you're testing equality against. 
	*/
	virtual bool equals(Object * other) {
		if (other == nullptr) return false;
        ObjectNode *o = dynamic_cast<ObjectNode*>(other);
		if (o == nullptr) {
			return false;
		}
        return this->data_->equals(o->data_);
	}
};