//LANGUAGE: CwC
#pragma once
#include <string.h>
#include "object.h"
#include "array.h"

/**
 * Set of useful helper methods for parsing Serialized data. Used primarily in deserialization to
 * get classNames, key values, and array values. 
 * 
 * Abstracts the necessity of each individual class dealing with and parsing the JSON char* representation
 */ 
/** authors: eldrid.s@husky.neu.edu & shetty.y@husky.neu.edu */
class JSONHelper {
public:
    /**
     * given a serialized substring, returns the first quoted value as the key for a payload
     */ 
    static String* getPayloadKey(char* s) {
        int len = strlen( s );
        bool nameStarted = false;
        char buff[100];
        int loc = 0;
        //pull the class name from the serialzed string
        for (int i = 0; i < len; i++){
            char temp = s[i];
            if(temp == '\'' && !nameStarted) {
                nameStarted = true;
            } else if(temp == '\'' && nameStarted) {
                //full name has been discovered
                break;
            } else if(nameStarted) {
                buff[loc] = temp;
                loc++;
            }
        }
        buff[loc] = '\0';
        char* className = buff;
        String* str = new String(className);
        return str;
    }
    /**
     * Given a serialized object, returns the top level value after the colon
     */ 
    static String* getPayloadValue(char* s) {
        int len = strlen( s );
        bool valueStarted = false;
        char objStart = '{';
        char objEnd = '}';
        int startCharDepth = 0;
        char buff[1024];
        int loc = 0;
        //find first colon as jumping off point:
        int colonIndex = 0;
        for(int i = 0; i < len; i++) {
            char temp=s[i];
            if(temp == ':') {
                colonIndex = i;
                break;
            }
        }
        //pull the top level object-value from the serialzed string
        for (int i = colonIndex; i < len; i++){
            char temp = s[i];
            if(!valueStarted) {
                //the three ways a payload in our JSON can start, will rely on this to know what character closes the value
                //also keeps track of depth if there are nested objects
                if(temp == '{' || temp == '[' || temp == '\'') {
                    objStart = temp;
                    startCharDepth = 1;
                    valueStarted = true;
                    if(temp == '{') {
                        objEnd = '}';
                    } else if(temp == '[') {
                        objEnd = ']';
                    } else if(temp == '\'') {
                        objEnd = '\'';
                    }   
                }
            } else if(temp == objStart && temp != objEnd && valueStarted) {
                startCharDepth++;
            } else if(temp == objEnd && valueStarted) {
                startCharDepth--;
                //if we've reached the final level of end character, break out of loop
                if(startCharDepth <= 0) {
                    break;
                }
            } else if(valueStarted) {
                buff[loc] = temp;
                loc++;
            }
        }
        buff[loc] = '\0';
        char* classValue = buff;
        String* str = new String(classValue);
        return str;
    }

    /**
     * Given the name of a specific key - returns the value in serialized substring or emptyString
     */ 
    static String* getValueFromKey(char* name, char* s) {
        int len = strlen( s );
        //pull the class name from the serialzed string
        int startIndex = 0;
        for (int i = 0; i < len; i++){
            //iterate through s, checking for each key until we have a match
            bool nameStarted = false;
            char buff[100];
            int loc = 0;
            for(int j = startIndex; j < len; j++) {
                char temp = s[j];
                if(temp == '\'' && !nameStarted) {
                    nameStarted = true;
                } else if(temp == '\'' && nameStarted) {
                    //full name has been discovered
                    startIndex = i;
                    break;
                } else if(nameStarted) {
                    buff[loc] = temp;
                    loc++;
                }
            }
            buff[loc] = '\0';
            char* keyName = buff;
            if(0 == strncmp(keyName, name, strlen(keyName))) {
                //we have a match!!!
                //get the specific starting place for getting value
                int objSize = len-startIndex;
                char* objectValue = new char[objSize];
                for(int z = 0; z < objSize; z++) {
                    objectValue[z] = s[z+startIndex];
                }
                return getPayloadValue(objectValue);
            }
        }
        return new String("");
    }

    /**
     * Given a serialized array value, returns number of unique items in it
     */
    static int arrayLen(char* s) {
        int len = strlen( s );
        bool valuesFound = false;
        char objStart = '{';
        char objEnd = '}';
        int count = 0;
        int startCharDepth = -1;
        //pull the top level object-value from the serialzed string
        for (int i = 0; i < len; i++){
            char temp = s[i];
            if(!valuesFound) {
                //the three ways a payload in our JSON can start, will rely on this to know what character closes the value
                //also keeps track of depth if there are nested objects
                if(temp == '{' || temp == '[' || temp == '\'') {
                    objStart = temp;
                    startCharDepth = 1;
                    valuesFound = true;
                    if(temp == '{') {
                        objEnd = '}';
                    } else if(temp == '[') {
                        objEnd = ']';
                    } else if(temp == '\'') {
                        objEnd = '\'';
                        startCharDepth = 0;
                    }   
                }
            } else if(temp == objStart) {
                startCharDepth++;
            } 
            if(temp == objEnd && valuesFound) {
                if(temp != objStart) {
                    startCharDepth--;
                }
                //if we've reached the final level of end character, break out of loop
                if(startCharDepth <= 0  || (temp == objStart && startCharDepth % 2 == 0)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Given a serialized array of values from JSON and an index, return string rep of that index
     */ 
    static String* getArrayValueAt(char* s, int index) {
       int len = strlen( s );
        bool valueStarted = false;
        char objStart = '{';
        char objEnd = '}';
        int startCharDepth = 0;
        char buff[1024];
        int loc = 0;
        int currIndex = -1;
        //pull the top level object-value from the serialzed string
        for (int i = 0; i < len; i++){
            char temp = s[i];

            if(!valueStarted) {
                //the three ways a payload in our JSON can start, will rely on this to know what character closes the value
                //also keeps track of depth if there are nested objects
                if(temp == '{' || temp == '[' || temp == '\'') {
                    objStart = temp;
                    startCharDepth = 1;
                    valueStarted = true;
                    if(temp == '{') {
                        objEnd = '}';
                    } else if(temp == '[') {
                        objEnd = ']';
                    } else if(temp == '\'') {
                        objEnd = '\'';
                        startCharDepth = 0;
                    }   
                }
            } else if(temp == objStart) {
                startCharDepth++;
            } 
            if(temp == objEnd && valueStarted) {
                if(temp != objStart) {
                    startCharDepth--;
                }
                //if we've reached the final level of end character, break out of loop
                if(startCharDepth <= 0  || (temp == objStart && startCharDepth % 2 == 0)) {
                    currIndex++;
                    if(currIndex > index) {
                        break;
                    }
                }
            } else if(valueStarted && index == currIndex) {
                if(objStart != objEnd || startCharDepth % 2 == 0) {
                    buff[loc] = temp;
                    loc++;
                }
            }
        }
        buff[loc] = '\0';
        char* classValue = buff;
        String* str = new String(classValue);
        return str;
    }
};