//LANGUAGE: CwC
#pragma once
#include <string.h>
#include "../object.h"
#include "jsonHelper.h"
#include "../utils/string.h"

/**
 * A serialization class that provides all the necessary formatting for taking
 * an object and storing each individual field into a JSON-esque object.
 * 
 * Use initSerialize to begin the formatting and input the object type
 * Write() will add values to the buffer and their keys
 * endSerialize() closes the brackets and should be the last thing called before get()
 * get() returns the char*
 */ 
/** authors: eldrid.s@husky.neu.edu & shetty.y@husky.neu.edu */
class Serializable {
public:
    StrBuff* buff;
    
    /**
     * Creates a Serializable Object, initializes the StrBuff
     */ 
    Serializable() {
        buff = new StrBuff();
    }

    ~Serializable() {
        delete buff;
    }
    /**
     * Returns the char* representation of the StrBuff used to store the buffer of the serialization
     * Object should be deleted after this method is called
     */ 
    char* get() {
        return buff->get()->c_str();
    }
    /**
     * Prints the current value of the buff to console
     */ 
    void print() {
        std::cout<<buff->val_<<"\n";
    }
    /**
     * Given the class name for a given object, begins to construct the serialized representation
     */ 
    void initSerialize(const char* cName) {
        char str[1000];
        sprintf(str, "{ '%s' : { ", cName);
        buff->c(str);
    }

    /**
     *  Write methods given a key name and value will add them in JSON format to the buffer
     */ 
    void write(const char* name, int val) {
        char str[1000];
        sprintf(str, "'%s' : '%i', ", name, val);
        buff->c(str);
    }

    void write(const char* name, float val) {
        char str[1000];
        sprintf(str, "'%s' : '%f', ", name, val);
        buff->c(str);
    }

    void write(const char* name, double val) {
        char str[1000];
        sprintf(str, "'%s' : '%f', ", name, val);
        buff->c(str);
    }

    void write(const char* name, String* val) {
        char str[1000];
        sprintf(str, "'%s' : '%s', ", name, val->c_str());
        buff->c(str);
    }

    void write(const char* name, size_t val) {
        char str[1000];
        sprintf(str, "'%s' : '%zu', ", name, val);
        buff->c(str);
    }
    /**
     * Special set of write commands for creating array data
     */ 
    void write(const char* name, int* val, int len) {
        char str[1000];
        sprintf(str, "'%s' : [ ", name);
        buff->c(str);
        for(int i = 0; i < len; i++) {
            sprintf(str, "'%i',", val[i]);
            buff->c(str);
        }
        buff->c("], ");
    }

    void write(const char* name, bool* val, int len) {
        char str[1000];
        sprintf(str, "'%s' : [ ", name);
        buff->c(str);
        for(int i = 0; i < len; i++) {
            sprintf(str, "'%d',", val[i]);
            buff->c(str);
        }
        buff->c("], ");
    }

    void write(const char* name, char* val) {
        int SIZEOFINSERTS = 10;
        char str[strlen(val) + strlen(name) + SIZEOFINSERTS];
        sprintf(str, "'%s' : '%s', ", name, val);
        buff->c(str);
    }

    void write(const char* name, char* val, bool wrap) {
        int SIZEOFINSERTS = 10;
        char str[strlen(val) + strlen(name) + SIZEOFINSERTS];
        if(wrap) {
            sprintf(str, "'%s' : '%s', ", name, val);
        } else {
            sprintf(str, "'%s' : %s, ", name, val);
        }
        buff->c(str);
    }//includes boolean flag to signify if this is a "wrapped" object (or a non-primative serialized)

    void write(const char* name, size_t* val, int len) {
        char str[1000];
        sprintf(str, "'%s' : [ ", name);
        buff->c(str);
        for(int i = 0; i < len; i++) {
            sprintf(str, "'%zu',", val[i]);
            buff->c(str);
        }
        buff->c("], ");
    }

    void write(const char* name, float* val, int len) {
        char str[1000];
        sprintf(str, "'%s' : [ ", name);
        buff->c(str);
        for(int i = 0; i < len; i++) {
            sprintf(str, "'%f',", val[i]);
            buff->c(str);
        }
        buff->c("], ");
    }

    void write(const char* name, double* val, int len) {
        char str[1000];
        sprintf(str, "'%s' : [ ", name);
        buff->c(str);
        for(int i = 0; i < len; i++) {
            sprintf(str, "'%f',", val[i]);
            buff->c(str);
        }
        buff->c("], ");
    }

    void write(const char* name, String** val, int len) {
        char str[1000];
        sprintf(str, "'%s' : [ ", name);
        buff->c(str);
        for(int i = 0; i < len; i++) {
            sprintf(str, "'%s',", val[i]->c_str());
            buff->c(str);
        }
        buff->c("], ");
    }

    void write(const char* name, Object** val, int len) {
        char str[100000];
        sprintf(str, "'%s' : [ ", name);
        buff->c(str);
        for(int i = 0; i < len; i++) {
            char* serial = val[i]->serialize();
            sprintf(str, "%s,", serial);
            buff->c(str);
        }
        buff->c("], ");
    }
    
    // void write(const char* name, Array* arr) {
    //     char * seralizedArr = arr->serialize();
    //     char str[10000];
    //     sprintf(str, "'%s' : '%s',", name, seralizedArr);
    // }

    // void write(const char* name, Schema* sch) {
    //     char * schemaSerialized = sch->serialize();
    //     char str[10000];
    //     sprintf(str, "'%s' : '%s',", name, schemaSerialized);
    // }

    /**
     * Called to close brackets of JSON after all necessary data written
     */ 
    void endSerialize() {
        buff->c(" } }");
    }
    /**
     * Given an Object, calls that class's serialize method to return a string of charbytes
     */ 
    static char* serialize(Object o) {
        return o.serialize();
    };
};