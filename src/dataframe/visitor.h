#pragma once
#include "row.h"
class Visitor {
    public:
    virtual void visit(Row & r) {}
};

class Reader {
    public:
    virtual bool visit(Row & r) {}
};

class Writer : public Visitor {
    public:
    void visit(Row & r) {}
    virtual bool done() {}
};