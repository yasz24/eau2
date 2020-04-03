#pragma once
#include "../object.h"

class KVStore;
class Deserializable {
public:
    Object* deserialize(char* s);
    Object* deserialize(char* s, KVStore* kv);
};