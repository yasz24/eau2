//LANGUAGE: CwC
#pragma once
#include <string.h>
#include "../object.h"
#include "jsonHelper.h"
#include "string.h"
#include "../dataframe/dataframe.h"
#include "../dataframe/schema.h"


class IntArray;
class FloatArray;
class StringArray;
class DoubleArray;
class StringColumn;
class Array;
/**
 * A generic Deserialize method that calls the correct deserialization method of the given serialized class
 * 
 * A different approach to deserialization than using specific constructors. Offered as a prototype that could be built
 * out further to include the Network classes
 */ 
/** authors: eldrid.s@husky.neu.edu & shetty.y@husky.neu.edu */
class Deserializable {
public:
    /**
     * Given a properly serialized string of bytes - returns the specific deserialization method for the serialized class
     */ 
    static Object* deserialize(char* s) {
        String* classString = JSONHelper::getPayloadKey(s);
        char* className = classString->c_str();

        String* valueString = JSONHelper::getPayloadValue(s);
        char* valueName = valueString->c_str();
        //figure out correct deserialization method to call...
        if(0 == strncmp(className,"IntArray", strlen(className))) {
            return IntArray::deserialize(valueName);
        } else if(0 == strncmp(className,"FloatArray", strlen(className))) {
            return FloatArray::deserialize(valueName);
        } else if(0 == strncmp(className,"StringArray", strlen(className))) {
            return StringArray::deserialize(valueName);
        } else if(0 == strncmp(className,"DoubleArray", strlen(className))) {
            return DoubleArray::deserialize(valueName);
        } else if(0 == strncmp(className,"StringColumn", strlen(className))) {
            return StringColumn::deserialize(valueName);
        } else if(0 == strncmp(className,"FloatColumn", strlen(className))) {
            return FloatColumn::deserialize(valueName);
        } else if(0 == strncmp(className,"DoubleColumn", strlen(className))) {
            return DoubleColumn::deserialize(valueName);
        } else if(0 == strncmp(className,"IntColumn", strlen(className))) {
            return IntColumn::deserialize(valueName);
        } else if(0 == strncmp(className,"BoolColumn", strlen(className))) {
            return BoolColumn::deserialize(valueName);
        } else if(0 == strncmp(className, "DataFrame", strlen(className))) {
            return DataFrame::deserialize(valueName);
        } else if(0 == strncmp(className, "Schema", strlen(className))) {
            return Schema::deserialize(valueName);
        } else if(0 == strncmp(className, "Array", strlen(className))) {
            return Array::deserialize(valueName);
        } else if(0 == strncmp(className, "Map", strlen(className))) {
            return Map::deserialize(valueName);
        } else if(0 == strncmp(className, "Queue", strlen(className))) {
            return Queue::deserialize(valueName);
        } else if(0 == strncmp(className, "KeyVal", strlen(className))) {
            return KeyVal::deserialize(valueName);
        } else {
            std::cout<<"ERROR: Classname picked up: "<<className<<"\n";
        }
        return nullptr;
    };
};