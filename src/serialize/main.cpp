//lang::CwC 
#pragma once
#include "object.h"  // Your file with the CwC declaration of Object
#include "string.h"  // Your file with the String class
#include "array.h"
#include "serial.h"
#include "network.h"
#include "deserialize.h"

#include <iostream>
/**
 * Tests the various serialization and deserialization of all required classes.
 * 
 * The arrays use the Deserializable class to correctly identify object based on JSON serialization
 * 
 * Network classes (located in network.h) use specialized constructors that consume a JSON representation of their class
 */ 

/**
 * Tests StringArray serialization and deserialization by comparing the serialization of a StringArray
 * to the serialized representation of a StringArray built by deserializing the initial array
 * 
 * Also tests the serialization of the intitial array to an expected output
 */ 
void testStringArraySerialization() {
    Sys* tester = new Sys();
    StringArray* is = new StringArray();
    is->pushBack(new String("taco"));
    is->pushBack(new String("dorito"));
    is->pushBack(new String("nacho"));
    is->pushBack(new String("enchilada"));
    char* string_is_serialized = is->serialize();
    StringArray* is_two = dynamic_cast<StringArray*>(Deserializable::deserialize(string_is_serialized));
    tester->t_true(strcmp(string_is_serialized, is_two->serialize()) == 0);
    char* expected = "{ 'StringArray' : { 'listLength_' : '4', 'arraySize_' : '32', 'vals_' : [ 'taco','dorito','nacho','enchilada',],  } }";
    tester->t_true(strcmp(string_is_serialized, expected) == 0);
    delete is;
    tester->OK("Passed String Array Serialization Tests");
    delete tester;
}

/**
 * Tests FloatArray serialization and deserialization by comparing the serialization of a FloatArray
 * to the serialized representation of a FloatArray built by deserializing the initial array
 * 
 */ 
void testFloatArraySerialization() {
    Sys* tester = new Sys();
    FloatArray* fa = new FloatArray(60);
    for(int i = 0; i < 50; i++) {
        fa->pushBack(i);
    }
    char* float_is_serialized = fa->serialize();
    FloatArray* fa_two = dynamic_cast<FloatArray*>(Deserializable::deserialize(float_is_serialized));
    tester->t_true(strcmp(float_is_serialized, fa_two->serialize()) == 0);
    delete fa;
    tester->OK("Passed Float Array Serialization Tests");
    delete tester;
}

/**
 * Tests IntArray serialization and deserialization by comparing the serialization of a IntArray
 * to the serialized representation of a IntArray built by deserializing the initial array
 * 
 */ 
void testIntArraySerialization() {
    Sys* tester = new Sys();

    IntArray* ia = new IntArray(120);
    for(int i = 0; i < 100; i++) {
        ia->pushBack(i);
    }
    char* int_is_serialized = ia->serialize();
    IntArray* ia_two = dynamic_cast<IntArray*>(Deserializable::deserialize(int_is_serialized));
    tester->t_true(strcmp(int_is_serialized, ia_two->serialize()) == 0);
    delete ia;
    tester->OK("Passed Int Array Serialization Tests");
    delete tester;
}

/**
 * Tests Message serialization and deserialization by comparing the serialization of a Message
 * to the serialized representation of a Message built by deserializing the initial message
 * 
 * Also compares serialized message to expected JSON value
 * 
 */ 
void testMessageSerialization() {
    Sys* tester = new Sys();
    Message* msg = new Message(1, 2, 3, 5);
    char* msg_serialized = msg->serialize();
    Message* msg_two = new Message(msg_serialized);
    char* expected = "{ 'Message' : { 'kind_' : '1', 'sender_' : '2', 'target_' : '3', 'id_' : '5',  } }";
    tester->t_true(strcmp(msg_serialized, msg_two->serialize()) == 0);
    tester->t_true(strcmp(msg_serialized, expected) == 0);
    tester->OK("Passed Message Serialization Tests");
    delete tester;
    delete msg;
}

/**
 * Tests Directory serialization and deserialization by comparing the serialization of a Directory
 * to the serialized representation of a Directory built by deserializing the initial directory
 * 
 */ 
void testDirectorySerialization() {
    Sys* tester = new Sys();
    size_t ports[5] = {1,2,4,5,6};
    String* sa = new String("mama mia");
    String* sb = new String("here we");
    String* sc = new String("go");
    String* sd = new String("again");
    String* strings[4] = {sa,sb,sc,sd};
    Directory* d = new Directory(1,2,3,5,6, ports, strings);
    char* d_serialized = d->serialize();
    Directory* d_two = new Directory(d_serialized);
    char* d_serialized_two = d_two->serialize();
    tester->t_true(strcmp(d_serialized, d_two->serialize()) == 0);
    tester->OK("Passed Directory Serialization Tests!");
    delete tester;
}

int main(int argc, char **argv) {
    testIntArraySerialization();
    testStringArraySerialization();
    testFloatArraySerialization();
    testMessageSerialization();
    testDirectorySerialization();
    return 0;
}