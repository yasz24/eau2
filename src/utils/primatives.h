//lang::CwC 
#pragma once
#include "../object.h"
//authors: eldrid.s@husky.neu.edu shetty.y@husky.neu.edu
//todo: primative destructors.

/**
 * Integer: a class that represents an int as an Object.
 * author: shetty.y@husky.neu.edu
*/
class Integer : public Object {
public:
	int val_; //val_ is to be owned by Integer, no one else has a pointer to it.

	Integer(int c) {
		this->val_ = c;
	}

	size_t hash() {
		return val_*100;
	}

	virtual bool equals(Object * other) {
		if (other == nullptr) return false;
		Integer *i = dynamic_cast<Integer*>(other);
        if(i == nullptr) return false;
		return this->val_ == i->val_;
	};

	~Integer() {
	}
};

class Boolean : public Object {
    public:
    bool val_;

    Boolean(bool b) {
        this->val_ = b;
    }

    virtual size_t hash_me_() {
        if(this->val_) {
            return 0;
        }
        return 1;
    }

    virtual bool equals(Object * other) {
        if (other == nullptr) return false;
		Boolean *b = dynamic_cast<Boolean*>(other);
        if(b == nullptr) return false;
		return (this->val_ == b->val_) || (!this->val_ == !b->val_);
    };

    ~Boolean() {
        delete this;
    }
};

class Float : public Object {
    public:
    float val_;

    Float(float f) {
        this->val_ = f;
    }

    virtual size_t hash_me_() {
        return (int)((this->val_)*100);
    }

    virtual bool equals(Object * other) {
        if (other == nullptr) return false;
		Float *f = dynamic_cast<Float*>(other);
        if(f == nullptr) return false;
		return (this->val_ == f->val_);
    };

    ~Float() {
        delete this;
    }
};