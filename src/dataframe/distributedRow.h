//lang::CwC
#pragma once
#include "schema.h"
#include "column.h"
#include "distributedColumn.h"
#include "fielder.h"
#include "../store/kvstore.h"

//authors: eldrid.s@husky.neu.edu shetty.y@husky.neu.edu
/*************************************************************************
 * DistributedRow::
 *
 * This class represents a single DistributedRow of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete DistributedRows. Internally a dataframe hold data in columns.
 * DistributedRows have pointer equality.
 */
class DistributedRow : public Object {
 public:
  Schema schema;
  Column** val_;
  size_t dfOffset = -1;
  //DistributedRow is an array of cols of size 1.
  /** Build a DistributedRow following a schema. */
  DistributedRow(Schema& scm, KVStore* kv) {
    val_ = new Column*[scm.width()];
    schema = scm;
    for(int i = 0; i < scm.width(); i++) {
      char type = scm.col_type(i);
      if(type == 'I') {
        val_[i] = new DistributedIntColumn(kv);
        val_[i]->push_back(2);
      } else if(type == 'F') {
        val_[i] = new DistributedFloatColumn(kv);
        val_[i]->push_back(0.0);
      } else if(type == 'S') {
        val_[i] = new DistributedStringColumn(kv);
        val_[i]->push_back("a");
      } else if(type == 'B') {
        val_[i] = new DistributedBoolColumn(kv);
        val_[i]->push_back(false);
      } else if(type == 'D') {
        val_[i] = new DistributedDoubleColumn(kv);
        val_[i]->push_back(0.0);
      } else {
        std::cout<<"ERROR: TYPE CLAIMS TO BE: "<<type<<"...WHICH IS NOT A VALID TYPE\n";
      }
    }
  };

  ~DistributedRow() {
    //delete[] val_;
    //gotta delete those columns fyi
  }
 
  /** Setters: set the given column with the given value. Setting a column with
    * a value of the wrong type results in no data change. */
  /** trying to access a column outside the width of the schema results in an assert failure */
  void set(size_t col, int val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'I') {
      DistributedIntColumn* ic = val_[col]->as_dist_int();
      ic->set(0, val);
    }
  };
  void set(size_t col, double val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'D') {
      DistributedDoubleColumn* dc = val_[col]->as_dist_double();
      dc->set(0, val);
    }
  };
  void set(size_t col, float val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'F') {
      DistributedFloatColumn* ic = val_[col]->as_dist_float();
      ic->set(0, val);
    }
  };
  void set(size_t col, bool val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'B') {
      DistributedBoolColumn* ic = val_[col]->as_dist_bool();
      ic->set(0, val);
    }
  };
  /** The string is external. */
  void set(size_t col, String* val) {
    assert(col < schema.width());
    if(val_[col]->get_type() == 'S') {
      DistributedStringColumn* ic = val_[col]->as_dist_string();
      ic->set(0, val);
    }
  };
 
  /** Set/get the index of this DistributedRow (ie. its position in the dataframe. This is
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
    DistributedIntColumn* ic = val_[col]->as_dist_int();
    return ic->get(0);
  };

  double get_double(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'D');
    DistributedDoubleColumn* dc = val_[col]->as_dist_double();
    return dc->get(0);
  };

  bool get_bool(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'B');
    DistributedBoolColumn* bc = val_[col]->as_dist_bool();
    return bc->get(0);
  };

  float get_float(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'F');
    DistributedFloatColumn* bc = val_[col]->as_dist_float();
    return bc->get(0);
  };

  String* get_string(size_t col) {
    assert(col < schema.width());
    assert(col_type(col) == 'S');
    DistributedStringColumn* bc = val_[col]->as_dist_string();
    return bc->get(0);
  };
 
  /** Number of fields in the DistributedRow. */
  size_t width() {
    return schema.width();
  };
 
   /** Type of the field at the given position. An idx >= width is  undefined. */
  char col_type(size_t idx) {
    assert(idx < schema.width());
    return val_[idx]->get_type();
  };
 
  /** Given a Fielder, visit every field of this DistributedRow. The first argument is
    * index of the DistributedRow in the dataframe.
    * Calling this method before the DistributedRow's fields have been set is undefined. */
  void visit(size_t idx, Fielder& f) {
    set_idx(idx);
    f.start(this->get_idx());
    for(int i = 0; i < width(); i++) {
      char type = val_[i]->get_type();
      switch (type) {
        case 'I': f.accept(val_[i]->as_dist_int()->get(0));
          break;
        case 'F': f.accept(val_[i]->as_dist_float()->get(0));
          break;
        case 'B': f.accept(val_[i]->as_dist_bool()->get(0));
          break;
        default: f.accept(val_[i]->as_dist_string()->get(0));
          break;
      }
    }
    f.done();
  };

  /**
   * Return a copy of the schema for this DistributedRow. Users responsibility to free the memory.
  */
  Schema* get_schema() {
    return new Schema(this->schema);
  }
};