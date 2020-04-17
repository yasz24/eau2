//lang::CwC
#pragma once
#include "schema.h"
#include "column.h"
#include "fielder.h"

//authors: eldrid.s@husky.neu.edu shetty.y@husky.neu.edu
/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */
class Row : public Object {
 public:
  Schema schema;
  Column** val_;
  size_t dfOffset = -1;
  //row is an array of cols of size 1.
  /** Build a row following a schema. */
  Row(Schema& scm) {
    std::cout<<scm.val_<<"\n";
    val_ = new Column*[scm.width()];
    schema = scm;
    
    for(int i = 0; i < scm.width(); i++) {
      char type = scm.col_type(i);
      if(type == 'I') {
        val_[i] = new IntColumn(1, 2);
      } else if(type == 'F') {
        val_[i] = new FloatColumn(1, 0.0);
      } else if(type == 'S') {
        val_[i] = new StringColumn(1, "a");
      } else if(type == 'B') {
        val_[i] = new BoolColumn(1, false);
      } else if(type == 'D') {
        val_[i] = new DoubleColumn(1, 0.0);
      } else {
        std::cout<<"ERROR: TYPE CLAIMS TO BE: "<<type<<"...WHICH IS NOT A VALID TYPE\n";
      }
    }
  };

  ~Row() {
    //delete[] val_;
    //gotta delete those columns fyi
  }
 
  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type results in no data change. */
  /** trying to access a column outside the width of the schema results in an assert failure */
  void set(size_t col, int val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'I') {
      IntColumn* ic = val_[col]->as_int();
      ic->set(0, val);
    }
  };
  void set(size_t col, double val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'D') {
      DoubleColumn* dc = val_[col]->as_double();
      dc->set(0, val);
    }
  };
  void set(size_t col, float val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'F') {
      FloatColumn* ic = val_[col]->as_float();
      ic->set(0, val);
    }
  };
  void set(size_t col, bool val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'B') {
      BoolColumn* ic = val_[col]->as_bool();
      ic->set(0, val);
    }
  };
  /** The string is external. */
  void set(size_t col, String* val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'S') {
      StringColumn* ic = val_[col]->as_string();
      ic->set(0, val);
    }
  };
 
  /** Set/get the index of this row (ie. its position in the dataframe. This is
   *  only used for informational purposes, unused otherwise */
  void set_idx(size_t idx) {
    dfOffset = idx;
  };
  size_t get_idx() {
    return dfOffset;
  };
 
  /** Getters: get the value at the given column. If the column is not
    * of the requested type, triggers assert error. */
  /** if col is outside the schema width, triggers assert error */
  int get_int(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'I');
    IntColumn* ic = val_[col]->as_int();
    return ic->get(0);
  };

  double get_double(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'D');
    DoubleColumn* dc = val_[col]->as_double();
    return dc->get(0);
  };

  bool get_bool(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'B');
    BoolColumn* bc = val_[col]->as_bool();
    return bc->get(0);
  };

  float get_float(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'F');
    FloatColumn* bc = val_[col]->as_float();
    return bc->get(0);
  };

  String* get_string(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'S');
    StringColumn* bc = val_[col]->as_string();
    return bc->get(0);
  };
 
  /** Number of fields in the row. */
  size_t width() {
    return schema.width();
  };
 
   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) {
    assert(idx < schema.width());
    return val_[idx]->get_type();
  };
 
  /** Given a Fielder, visit every field of this row. The first argument is
    * index of the row in the dataframe.
    * Calling this method before the row's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    set_idx(idx);
    f.start(this->get_idx());
    for(int i = 0; i < width(); i++) {
      char type = val_[i]->get_type();
      switch (type) {
        case 'I': f.accept(val_[i]->as_int()->get(0));
          break;
        case 'F': f.accept(val_[i]->as_float()->get(0));
          break;
        case 'B': f.accept(val_[i]->as_bool()->get(0));
          break;
        default: f.accept(val_[i]->as_string()->get(0));
          break;
      }
    }
    f.done();
  };

  /**
   * Return a copy of the schema for this row. Users responsibility to free the memory.
  */
  Schema* get_schema() {
    return new Schema(schema);
  }
};