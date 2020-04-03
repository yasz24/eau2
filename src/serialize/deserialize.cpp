//LANGUAGE: CwC
#pragma once
#include <string.h>
#include "../object.h"
#include "jsonHelper.h"
#include "string.h"
#include "../network/network.h"
#include "../dataframe/dataframe.h"
#include "../dataframe/schema.h"
#include "../utils/distributedArray.h"
#include "../dataframe/distributedDataframe.h"
#include "../dataframe/distributedColumn.h"

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
            std::cout << "in deserial\n";
            return new Schema(valueName);
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
        } else if(0 == strncmp(className, "Message", strlen(className))) {
            return new Message(s);
        } else if(0 == strncmp(className, "Register", strlen(className))) {
            return new Register(s);
        } else if(0 == strncmp(className, "Directory", strlen(className))) {
            return new Directory(s);
        } else {
            std::cout<<"ERROR: Classname picked up: "<<className<<"\n";
        }
        return nullptr;
    };

    Object* Deserializable::deserialize(char* s, KVStore* kv) {
        String* classString = JSONHelper::getPayloadKey(s);
        char* className = classString->c_str();
        String* valueString = JSONHelper::getPayloadValue(s);
        char* valueName = valueString->c_str();
        //figure out correct deserialization method to call...
        if (0 == strncmp(className, "Array", strlen(className))) {
            return new Array(s, kv);
        } else if(0 == strncmp(className, "IntDistributedArray", strlen(className))) {
            return new IntDistributedArray(s, kv);
        } else if(0 == strncmp(className, "FloatDistributedArray", strlen(className))) {
            return new FloatDistributedArray(s, kv);
        } else if(0 == strncmp(className, "DoubleDistributedArray", strlen(className))) {
            return new DoubleDistributedArray(s, kv);
        } else if(0 == strncmp(className, "BoolDistributedArray", strlen(className))) {
            return new BoolDistributedArray(s, kv);
        } else if(0 == strncmp(className, "StringDistributedArray", strlen(className))) {
            return new StringDistributedArray(s, kv);
        } else if(0 == strncmp(className, "DistributedDataFrame", strlen(className))) {
            return new DistributedDataFrame(s, kv);
        } else if(0 == strncmp(className, "DistributedIntColumn", strlen(className))) {
            return new DistributedIntColumn(s, kv);
        } else if(0 == strncmp(className, "DistributedDoubleColumn", strlen(className))) {
            return new DistributedDoubleColumn(s, kv);
        } else if(0 == strncmp(className, "DistributedFloatColumn", strlen(className))) {
            return new DistributedFloatColumn(s, kv);
        } else if(0 == strncmp(className, "DistributedBoolColumn", strlen(className))) {
            return new DistributedBoolColumn(s, kv);
        } else if(0 == strncmp(className, "DistributedStringColumn", strlen(className))) {
            return new DistributedStringColumn(s, kv);
        } else if(0 == strncmp(className, "DistributedDataFrame", strlen(className))) {
            return new DistributedDataFrame(s, kv);
        } else {
            std::cout<<"ERROR: Classname picked up: "<<className<<"\n";
        }
        return nullptr;
    };

        