#include "../object.h"
//todo: write hash, equals
class Value : public Object{
public:
    char* data; //owned
    size_t length; 

    Value(char* data, size_t length) {
        this->data = data;
        this->length = length;
    }
};