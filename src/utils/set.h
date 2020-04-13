#pragma once
#include "../dataframe/distributedDataframe.h"

/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set {
public:  
  bool* vals_;  // owned; data
  size_t size_; // number of elements

  /** Creates a set of the same size as the dataframe. */ 
  Set(DistributedDataFrame* df) : Set(df->nrows()) {}

  /** Creates a set of the given size. */
  Set(size_t sz) :  vals_(new bool[sz]), size_(sz) {
      for(size_t i = 0; i < size_; i++)
          vals_[i] = false; 
  }

  ~Set() { delete[] vals_; }

  /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
   *  values can occur if there are references to pids or uids in commits
   *  that did not appear in projects or users.
   */
  void set(size_t idx) {
    if (idx >= size_ ) return; // ignoring out of bound writes
    vals_[idx] = true;       
  }

  /** Is idx in the set?  See comment for set(). */
  bool test(size_t idx) {
    if (idx >= size_) return true; // ignoring out of bound reads
    return vals_[idx];
  }

  size_t size() { return size_; }

  /** Performs set union in place. */
  void union_(Set& from) {
    for (size_t i = 0; i < from.size_; i++) 
      if (from.test(i))
	set(i);
  }
};
