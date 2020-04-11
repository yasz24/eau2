#pragma once
#include "../utils/set.h"
#include "../dataframe/visitor.h"
/*******************************************************************************
 * A SetUpdater is a reader that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Reader {
public:
  Set& set_; // set to update
  
  SetUpdater(Set& set): set_(set) {}

  /** Assume a row with at least one column of type I. Assumes that there
   * are no missing. Reads the value and sets the corresponding position.
   * The return value is irrelevant here. */
  bool visit(Row & row) { set_.set(row.get_int(0));  return false; }

};
