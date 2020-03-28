//LANGUAGE: CwC
#pragma once
#include <string.h>
#include "../object.h"
#include "jsonHelper.h"
#include "string.h"
#include "../dataframe/dataframe.h"
#include "../dataframe/schema.h"
#include "../utils/distributedArray.h"

/**
 * A generic Deserialize method that calls the correct deserialization method of the given serialized class
 * 
 * A different approach to deserialization than using specific constructors. Offered as a prototype that could be built
 * out further to include the Network classes
 */
/** authors: eldrid.s@husky.neu.edu & shetty.y@husky.neu.edu */
    /**
     * Given a properly serialized string of bytes - returns the specific deserialization method for the serialized class
     */ 
    Object* Deserializable::deserialize(char* s) {
        String* classString = JSONHelper::getPayloadKey(s);
        char* className = classString->c_str();
        String* valueString = JSONHelper::getPayloadValue(s);
        char* valueName = valueString->c_str();
        //figure out correct deserialization method to call...
        if(0 == strncmp(className,"IntArray", strlen(className))) {
            return new IntArray(s);
        } else if(0 == strncmp(className,"FloatArray", strlen(className))) {
            return new FloatArray(s);
        } else if(0 == strncmp(className,"StringArray", strlen(className))) {
            return new StringArray(s);
        } else if(0 == strncmp(className,"DoubleArray", strlen(className))) {
            return new DoubleArray(s);
        } else if(0 == strncmp(className,"StringColumn", strlen(className))) {
            return new StringColumn(s);
        } else if(0 == strncmp(className,"FloatColumn", strlen(className))) {
            return FloatColumn::deserialize(valueName);
        } else if(0 == strncmp(className,"DoubleColumn", strlen(className))) {
            return DoubleColumn::deserialize(valueName);
        } else if(0 == strncmp(className,"IntColumn", strlen(className))) {
            return new IntColumn(s);
        } else if(0 == strncmp(className,"BoolColumn", strlen(className))) {
            return BoolColumn::deserialize(valueName);
        } else if(0 == strncmp(className, "DataFrame", strlen(className))) {
            return DataFrame::deserialize(valueName);
        } else if(0 == strncmp(className, "Schema", strlen(className))) {
            return Schema::deserialize(valueName);
        } else if(0 == strncmp(className, "Array", strlen(className))) {
            return new Array(s);
        } else if(0 == strncmp(className, "Map", strlen(className))) {
            return new Map(s);
        } else if(0 == strncmp(className, "Queue", strlen(className))) {
            return new Queue(s);
        } else if(0 == strncmp(className, "Key", strlen(className))) {
            return new Key(s);
        } else if(0 == strncmp(className, "KeyVal", strlen(className))) {
            return KeyVal::deserialize(valueName);
        } else if(0 == strncmp(className, "Value", strlen(className))) {
            return new Value(s);
        } else if(0 == strncmp(className, "KVStore", strlen(className))) {
            return new KVStore(s);
        } else if(0 == strncmp(className, "IntDistributedArray", strlen(className))) {
            return new IntDistributedArray(s);
        } else if(0 == strncmp(className, "FloatDistributedArray", strlen(className))) {
            return new FloatDistributedArray(s);
        } else if(0 == strncmp(className, "DoubleDistributedArray", strlen(className))) {
            return new DoubleDistributedArray(s);
        } else if(0 == strncmp(className, "BoolDistributedArray", strlen(className))) {
            return new BoolDistributedArray(s);
        } else if(0 == strncmp(className, "StringDistributedArray", strlen(className))) {
            return new StringDistributedArray(s);
        } else {
            std::cout<<"ERROR: Classname picked up: "<<className<<"\n";
        }
        return nullptr;
    };