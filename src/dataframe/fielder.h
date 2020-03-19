#pragma once

#include "../object.h"
#include "../utils/string.h"
//authors: shetty.y@husky.neu.edu and eldrid.s@husky.neu.edu
/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object {
public:
 
  /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
  virtual void start(size_t r) = 0;
 
  /** Called for fields of the argument's type with the value of the field. */
  virtual void accept(bool b) = 0;
  virtual void accept(float f) = 0;
  virtual void accept(int i) = 0;
  virtual void accept(String* s) = 0;
 
  /** Called when all fields have been seen. */
  virtual void done() = 0;
};