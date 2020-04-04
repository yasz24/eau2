#pragma once
#include "row.h"
class Reader {
    public:
    virtual bool visit(Row & r) { return false; }
};

class Writer {
    public:
    virtual void visit(Row & r) {};
    virtual bool done() { return true; };
};