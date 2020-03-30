#pragma once
#include "../object.h"
class Deserializable {
public:
    Object* deserialize(char* s);
};