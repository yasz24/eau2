//lang::CwC
#pragma once
#include "../object.h"
#include "string.h"
#include "../utils/map.h"
#include "../utils/primatives.h"
#include "../serialize/serial.h"
#include <iostream>

//authors: shetty.y@husky.neu.edu eldrid.s@husky.neu.edu

//todo: primatives destructors.
//todo: write destructors.
//todo: write hashcode for schema

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object {

public:  
    size_t capacity_ = 100; // default capacity.
    char* val_; //val_ is to be owned by string
    size_t empty_index_ = 0; // leftmost empty index in val. also number of columns.
    size_t num_rows_; // num of rows. 
    Map* col_name_idx; // Map<String, Integer>
    Map* row_name_idx; // Map<String, Integer>
    Map* col_idx_name; // Map<Integer, String>
    Map* row_idx_name; // Map<Integer, String>

    /** Copying constructor */
    Schema(Schema& from) {
        //std::cout<<"entered copy constructor\n";
        this->capacity_ = from.capacity_;
        this->empty_index_ = from.empty_index_;
        this->val_ = new char[this->capacity_ + 1];

        for (size_t i = 0; i < this->empty_index_; i++) {
            this->val_[i] = from.val_[i]; 
        }
        this->num_rows_ = from.num_rows_;
        //std::cout<<"upper bound\n";
        this->col_name_idx = from.col_name_idx->clone();
        //std::cout<<"lower bound\n";
        this->row_name_idx = from.row_name_idx->clone();
        this->col_idx_name = from.col_idx_name->clone();
        this->row_idx_name = from.row_idx_name->clone();
    } 
    
    /** Create an empty schema **/
    Schema() {
        this->val_ = new char[this->capacity_ + 1];
        //null terminate the string.
        this->val_[this->capacity_] = '\0';
        this->num_rows_ = 0;

        //initialize the row and col maps.
        this->col_name_idx = new Map();
        this->row_name_idx = new Map();
        this->col_idx_name = new Map();
        this->row_idx_name = new Map();
    }

    Schema(char* serialized) {
        char* payload = JSONHelper::getPayloadValue(serialized)->c_str();
        this->capacity_ = std::stoi(JSONHelper::getValueFromKey("capacity_", payload)->c_str());
        this->empty_index_ = std::stoi(JSONHelper::getValueFromKey("empty_index_", payload)->c_str());
        this->num_rows_ = std::stoi(JSONHelper::getValueFromKey("num_rows_", payload)->c_str());
        this->val_ = JSONHelper::getValueFromKey("val_", payload)->c_str();
        //for reasons
        this->col_name_idx = new Map();
        this->row_name_idx = new Map();
        this->col_idx_name = new Map();
        this->row_idx_name = new Map();
    }
    
    /** Create a schema from a string of types. A string that contains
        * characters other than those identifying the four type results in
        * undefined behavior. The argument is external, a nullptr argument 
        * breaks the program. **/
    Schema(const char* types) {
        assert(types != nullptr);
        bool schemaValid = true;
        size_t types_len =  strlen(types);
        //loop through character by character to validate schema.
        for (size_t i = 0; i < types_len; i++)
        {
            char c = types[i];
            //todo: find a better way to do this.
            if (!this->valid_col_char_(c)) {
                // the schema is invalid, create an empty schema.
                schemaValid = false;
                break;
            }
        }

        if (schemaValid) {
            if (this->capacity_ - 1 < types_len) {
                this->capacity_ = 2 * types_len;
            }
            //allocate a buffer.
            this->val_ = new char[this->capacity_ + 1];
            for (size_t i=0; i < types_len; i++) {
			    this->val_[i] = types[i];
		    }
            //set the empty_index_ to the length of types.
            this->empty_index_ = types_len;
        } else {
            this->val_ = new char[this->capacity_ + 1];
        }
        //null terminate the string.
        this->val_[this->capacity_] = '\0';

        //initialize num_rows
        this->num_rows_ = 0;
        //initialize the row and col maps.
        this->col_name_idx = new Map();
        this->row_name_idx = new Map();
        this->col_idx_name = new Map();
        this->row_idx_name = new Map();
    }

    /**
     * Destructor.
    */
    ~Schema() {
        delete[] val_;
    }
    
    /** Add a column of the given type and name (can be nullptr), name
        * is external. Names are expectd to be unique, duplicates result
        * the newly added column not getting a name. If type is invalid, no column added. */
    void add_column(char typ, String* name) {
        if (this->empty_index_ == this->capacity_) {
            resize_();
        }
        if (this->valid_col_char_(typ)) {
            //set the typ for the new column if valid.
            this->val_[this->empty_index_] = typ;
            if (name != nullptr) {
                if (this->col_name_idx->get(name) == nullptr) {
                    Integer* i = new Integer(this->empty_index_);
                    this->col_name_idx->add(name, i);
                    this->col_idx_name->add(i, name);
                }
            }
            //bump up the empty index
            this->empty_index_ += 1;
        }
    }
    
    /** Add a row with a name (possibly nullptr), name is external.  Names are
     *  expectd to be unique, duplicates result in newly added row not getting a name. */
    void add_row(String* name) {
        if (name != nullptr ) {
            if (this->row_name_idx->get(name) == nullptr) {
                Integer* i = new Integer(this->num_rows_);
                this->row_name_idx->add(name, i);
                this->row_idx_name->add(i, name);
            }
        }
        this->num_rows_ += 1;
    }
    
    /** Return name of row at idx; nullptr indicates no name. An idx >= width
        * is nullptr. */
    String* row_name(size_t idx) {
        if (idx >= this->num_rows_) return nullptr;
        Integer* i = new Integer(idx);
        Object* o = this->row_idx_name->get(i);
        delete i;
        if (o == nullptr) {
            return nullptr;
        } else {
            return dynamic_cast<String*>(o);
        } 
    }
    
    /** Return name of column at idx; nullptr indicates no name given.
        *  An idx >= width return nullptr.*/
    String* col_name(size_t idx) {
        if (idx >= this->empty_index_) return nullptr;

        Integer* i = new Integer(idx);
        Object* o = this->col_idx_name->get(i);
        delete i;
        if (o == nullptr) {
            return nullptr;
        } else {
            return dynamic_cast<String*>(o);
        } 
    }
    
    /** Return type of column at idx. An idx >= width is white space character. */
    char col_type(size_t idx) {
        char w = ' ';
        if (idx >= this->empty_index_) { return w; }
        return this->val_[idx];
    }
    
    /** Given a column name return its index, or -1. */
    int col_idx(const char* name) {
        String* s = new String(name);
        Object* o = this->col_name_idx->get(s);
        delete s;
        if (o != nullptr) {
            Integer* i = dynamic_cast<Integer*>(o);
            if (i != nullptr) {
                return i->val_; 
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
    
    /** Given a row name return its index, or -1. */
    int row_idx(const char* name) {
        String* s = new String(name);
        Object* o = this->row_name_idx->get(s);
        delete s;
        if (o != nullptr) {
            Integer* i = dynamic_cast<Integer*>(o);
            if (i != nullptr) {
                return i->val_; 
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
    
    /** The number of columns */
    size_t width() {
        return this->empty_index_;
    }
    
    /** The number of rows */
    size_t length() {
        return this->num_rows_;
    }

    void resize_() {
        size_t len = this->capacity_;
        //double size.
        len = this->capacity_ * 2;
        //create a new array that has double the size.
        char *copy_ = new char[len + 1];
        
        //copy over elements one by one.
        for (size_t i = 0; i < this->capacity_; i++) {
            copy_[i] = this->val_[i];
        }
        
        //need to free the array.
        delete[] this->val_;
        //need to get old pointer to point to new array. 
        this->val_ = copy_;

        //double the length.
        this->capacity_ = len;

        //null terminate.
        this->val_[capacity_] = '\0';
    }

    bool valid_col_char_(char c) {
        return (c == 'S' || c == 'B' || c == 'I' || c == 'F' || c == 'D');
    }

     /** Compare two Schema's. Does not consider number of rows in the schema*/
    bool equals(Object* other) {
        if (other == this) return true;
        Schema* x = dynamic_cast<Schema *>(other);
        if (x == nullptr) return false;
        if (this->width() != x->width()) return false;
        for (size_t i = 0; i < this->width(); i++) {
            if ( this->val_[i] != x->col_type(i)) { 
                return false; 
            }
        }
        return true;
    }

    /**
     * hashcode.
    */
    size_t hash() {
        size_t hash_ = 0;
        for (size_t i = 0; i < this->width(); i++) {
            hash_ += (i + 1) * this->val_[i]; 
        }
        
        return hash_;
    }

    //will lose the row/column names with serialization
    char* serialize() {
        Serializable* sb = new Serializable();
        sb->initSerialize("Schema");
        sb->write("capacity_", capacity_);
        sb->write("empty_index_", empty_index_);
        sb->write("num_rows_", num_rows_);
        sb->write("val_", this->val_);
        sb->endSerialize();
        char* value = sb->get();
        delete sb;
        return value;
    }
};