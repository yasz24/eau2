//lang::CwC
#pragma once
#include "../object.h"
#include "../store/key.h"
#include "../store/kvstore.h"
#include "schema.h"
#include "column.h"
#include "row.h"
#include "rower.h"
#include "../utils/primatives.h"
#include <iostream>
#include <thread>

//authors: eldrid.s@husky.neu.edu shetty.y@husky.neu.edu

/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object {
public:
    //A dataframe is an array of columns.
    Schema* schema_; //owned.
    Array* cols_; //owned.
  
    /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
    DataFrame(DataFrame& df) {
      this->schema_ = new Schema(df.get_schema());
      this->cols_ = new Array();
      for (size_t i = 0; i < this->schema_->width(); i++) {
        char col_type = this->schema_->col_type(i);
        Column* col;
        switch (col_type) {
        case 'I':
          col = new IntColumn();
          break;
        case 'S':
          col = new StringColumn();
          break;
        case 'B':
          col = new BoolColumn();
          break;
        case 'F':
          col = new FloatColumn();
          break;
        default:
          col = nullptr;
          break;
        }

        if (col != nullptr) {
          this->cols_->pushBack(col);
        }
      } 
    }
  
    /** Create a data frame from a schema and columns. All columns are created
      * empty. */
    DataFrame(Schema& schema) {
      this->schema_ = new Schema(schema);
      this->cols_ = new Array();
      for (size_t i = 0; i < this->schema_->width(); i++) {
        char col_type = this->schema_->col_type(i);
        Column* col;
        switch (col_type) {
        case 'I':
          col = new IntColumn();
          break;
        case 'S':
          col = new StringColumn();
          break;
        case 'B':
          col = new BoolColumn();
          break;
        case 'F':
          col = new FloatColumn();
          break;
        default:
          col = nullptr;
          break;
        }

        if (col != nullptr) {
          this->cols_->pushBack(col);
        }
      }

    }
  
    /** Returns the dataframe's schema as a copy Schema. Modifying the schema after a dataframe
      * wouldn't change internal schema. */
    Schema& get_schema() {
      return *(new Schema(*this->schema_));
    }
  
    /** Adds a column this dataframe, updates the schema, the new column
      * is external, and appears as the last column of the dataframe, the
      * name is optional and external. A nullptr colum does not create a new column. */
    void add_column(Column* col, String* name) {
      if (col == nullptr) {
        return;
      }
      size_t col_size = col->size();
      if (col_size > this->schema_->length()) {
        return;
      }
      char col_type = col->get_type();
      this->schema_->add_column(col_type, name);
      if (col_size < this->schema_->length()) {
        //padding columns with dummy data.
        switch (col_type) {
        case 'I': {
            IntColumn* intCol = col->as_int();
            for (size_t i = col_size; i < this->schema_->length(); i++) {
              intCol->push_back(0);
            }
            break;
          }
        case 'S': {
            StringColumn* strCol = col->as_string();
            for (size_t i = col_size; i < this->schema_->length(); i++) {
              strCol->push_back(nullptr);
            }
            break;
          }
        case 'B': {
            BoolColumn* boolCol = col->as_bool();
            for (size_t i = col_size; i < this->schema_->length(); i++) {
              boolCol->push_back(false);
            }
            break;
          }
        case 'F': {
            FloatColumn* floatCol = col->as_float();
            for (size_t i = col_size; i < this->schema_->length(); i++) {
              floatCol->push_back((float) 0);
            }
            break;
          }
        default:
          //do nothing.
          break;
        }
      }
      this->cols_->pushBack(col);
    }
  
    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds would exit*/
    int get_int(size_t col, size_t row) {
      assert(col < this->schema_->width());
      char col_type = this->schema_->col_type(col);
      assert(col_type == 'I');
      IntColumn* intCol = dynamic_cast<IntColumn*>(this->cols_->get(col));
      assert(row < this->schema_->length());
      return intCol->get(row);
    }

    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, return false*/
    bool get_bool(size_t col, size_t row) {
      assert(col < this->schema_->width());
      char col_type = this->schema_->col_type(col);
      assert(col_type == 'B');
      BoolColumn* boolCol = dynamic_cast<BoolColumn*>(this->cols_->get(col));
      assert(row < this->schema_->length());
      return boolCol->get(row);
    }

    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, return -1.0*/
    float get_float(size_t col, size_t row) {
      assert(col < this->schema_->width());
      char col_type = this->schema_->col_type(col);
      assert(col_type == 'F');
      FloatColumn* floatCol = dynamic_cast<FloatColumn*>(this->cols_->get(col));
      assert(row < this->schema_->length());
      return floatCol->get(row);
    }

    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, return nullptr*/
    String*  get_string(size_t col, size_t row) {
      assert(col < this->schema_->width());
      char col_type = this->schema_->col_type(col);
      assert(col_type == 'S');
      StringColumn* stringCol = dynamic_cast<StringColumn*>(this->cols_->get(col));
      assert(row < this->schema_->length());
      return stringCol->get(row);
    }
  
    /** Return the offset of the given column name or -1 if no such col. */
    int get_col(String& col) {
      return this->schema_->col_idx(col.c_str());
    }
  
    /** Return the offset of the given row name or -1 if no such row. */
    int get_row(String& col) {
      return this->schema_->row_idx(col.c_str());
    }
  
    /** Set the value at the given column and row to the given value.
      * If the column is not  of the right type or the indices are out of
      * bound, the result is to do nothing. */
    void set(size_t col, size_t row, int val) {
      if (col < this->schema_->width()) {
        char col_type = this->schema_->col_type(col);
        if (col_type == 'I') {
          IntColumn* intCol = dynamic_cast<IntColumn*>(this->cols_->get(col));
          if (row < this->schema_->length()) {
            intCol->set(row, val);
          } else {
            return;
          }
        } else {
          return;
        }
      } else {
        return;
      }
    }

    /** Set the value at the given column and row to the given value.
      * If the column is not  of the right type or the indices are out of
      * bound, the result is to do nothing. */
    void set(size_t col, size_t row, bool val) {
       if (col < this->schema_->width()) {
        char col_type = this->schema_->col_type(col);
        if (col_type == 'B') {
          BoolColumn* boolCol = dynamic_cast<BoolColumn*>(this->cols_->get(col));
          if (row < this->schema_->length()) {
            boolCol->set(row, val);
          } else {
            return;
          }
        } else {
          return;
        }
      } else {
        return;
      }
    }

    /** Set the value at the given column and row to the given value.
      * If the column is not  of the right type or the indices are out of
      * bound, the result is to do nothing. */
    void set(size_t col, size_t row, float val) {
      if (col < this->schema_->width()) {
        char col_type = this->schema_->col_type(col);
        if (col_type == 'F') {
          FloatColumn* floatCol = dynamic_cast<FloatColumn*>(this->cols_->get(col));
          if (row < this->schema_->length()) {
            floatCol->set(row, val);
          } else {
            return;
          }
        } else {
          return;
        }
      } else {
        return;
      }
    }

    /** Set the value at the given column and row to the given value.
      * If the column is not  of the right type or the indices are out of
      * bound, the result is to do nothing. */
    void set(size_t col, size_t row, String* val) {
      if (col < this->schema_->width()) {
        char col_type = this->schema_->col_type(col);
        if (col_type == 'S') {
          StringColumn* stringCol = dynamic_cast<StringColumn*>(this->cols_->get(col));
          if (row < this->schema_->length()) {
            stringCol->set(row, val);
          } else {
            return;
          }
        } else {
          return;
        }
      } else {
        return;
      }
    }
  
    /** Set the fields of the given row object with values from the columns at
      * the given offset.  If the row is not form the same schema as the
      * dataframe, then the row does not get filled.
      */
    void fill_row(size_t idx, Row& row) {
      Schema* row_schema = row.get_schema();
      bool schemas_equal = this->schema_->equals(row_schema);
      delete row_schema;
      if (schemas_equal) {
        if (idx < this->schema_->length()) {
          for (size_t i = 0; i < row.width(); i++) {
            char col_type = this->schema_->col_type(i);
            switch (col_type) {
            case 'I': {
                IntColumn* intCol = dynamic_cast<IntColumn*>(this->cols_->get(i));
                row.set(i, intCol->get(idx));
                break;
              }
            case 'S': {
                StringColumn* strCol = dynamic_cast<StringColumn*>(this->cols_->get(i));
                row.set(i, strCol->get(idx));
                break;
              }
            case 'B': {
                BoolColumn* boolCol = dynamic_cast<BoolColumn*>(this->cols_->get(i));
                row.set(i, boolCol->get(idx));        
                break;
              }
            case 'F': {
                FloatColumn* floatCol = dynamic_cast<FloatColumn*>(this->cols_->get(i));
                row.set(i, floatCol->get(idx));
                break;
              }
            default:
              //do nothing.
              break;
            }
          }
        } else {
          return;
        }
      } else {
        return;
      }
    }
  
    /** Add a row at the end of this dataframe. The row is expected to have
     *  the right schema and be filled with values, otherwise no row information added to the dataframe.  */
    void add_row(Row& row) {
      Schema* row_schema = row.get_schema();
      bool schemas_equal = this->schema_->equals(row_schema);
      delete row_schema;
      if (schemas_equal) {
        for (size_t i = 0; i < row.width(); i++) {
            char col_type = this->schema_->col_type(i);
            switch (col_type) {
            case 'I': {
                IntColumn* intCol = dynamic_cast<IntColumn*>(this->cols_->get(i));
                intCol->push_back(row.get_int(i));
                break;
              }
            case 'S': {
                StringColumn* strCol = dynamic_cast<StringColumn*>(this->cols_->get(i));
                strCol->push_back(row.get_string(i));
                break;
              }
            case 'B': {
                BoolColumn* boolCol = dynamic_cast<BoolColumn*>(this->cols_->get(i));
                boolCol->push_back(row.get_bool(i));       
                break;
              }
            case 'F': {
                FloatColumn* floatCol = dynamic_cast<FloatColumn*>(this->cols_->get(i));
                floatCol->push_back(row.get_float(i));  
                break;
              }
            default:
              //do nothing.
              break;
            }
        }
        //update the schema with a new row.
        this->schema_->add_row(nullptr);
      } else {
        return;
      }
    }
  
    /** The number of rows in the dataframe. */
    size_t nrows() {
      return this->schema_->length();
    }
  
    /** The number of columns in the dataframe.*/
    size_t ncols() {
      return this->schema_->width();
    }
  
    /** Visit rows in order */
    void map(Rower& r) {
      //create a new row.
      Row* row = new Row(*this->schema_);

      //apply the rower to each row.
      for (size_t i = 0; i < this->schema_->length(); i++) {
        this->fill_row(i, *row);
        row->set_idx(i);
        r.accept(*row);
      }
      delete row;
    }
    /**
     * A multi-threaded map that divides the work of iterating over a dataframe into two threads before combining their payload
     * In order to decrease the amount of code needed to be stored for the various threads, instead of sending the whole schema
     * we send a char* representation of the schema that is then converted in pTraversal
     * numThreads expects an int between 2 and 4, defaults to 2
     */
    void pmap(Rower& r, size_t numThreads) {
        if(numThreads == 4) {
          Rower* r2 = dynamic_cast<Rower*>(r.clone());
          Rower* r3 = dynamic_cast<Rower*>(r.clone());
          Rower* r4 = dynamic_cast<Rower*>(r.clone());
          int rowCount = nrows();
          Schema* scm = new Schema(this->get_schema());
          std::thread t1(&DataFrame::pTraversal, std::ref(*this), 0, std::cref(rowCount)/4, std::ref(scm), std::ref(r));
          std::thread t2(&DataFrame::pTraversal, std::ref(*this), std::cref(rowCount)/4, 2*(std::cref(rowCount)/4), std::ref(scm), std::ref(*r2));
          std::thread t3(&DataFrame::pTraversal, std::ref(*this), 2*(std::cref(rowCount)/4), 3*(std::cref(rowCount)/4), std::ref(scm), std::ref(*r3));
          std::thread t4(&DataFrame::pTraversal, std::ref(*this), 3*(std::cref(rowCount)/4), std::cref(rowCount), std::ref(scm), std::ref(*r4));
          t1.join();
          t2.join();
          t3.join();
          t4.join();
          r.join_delete(r2);
          r.join_delete(r3);
          r.join_delete(r4);
        } else if(numThreads == 3) {
          Rower* r2 = dynamic_cast<Rower*>(r.clone());
          Rower* r3 = dynamic_cast<Rower*>(r.clone());
          int rowCount = nrows();
          Schema* scm = new Schema(this->get_schema());
          std::thread t1(&DataFrame::pTraversal, std::ref(*this), 0, std::cref(rowCount)/3, std::ref(scm), std::ref(r));
          std::thread t2(&DataFrame::pTraversal, std::ref(*this), std::cref(rowCount)/3, 2*(std::cref(rowCount)/3), std::ref(scm), std::ref(*r2));
          std::thread t3(&DataFrame::pTraversal, std::ref(*this), 2*(std::cref(rowCount)/3), std::cref(rowCount), std::ref(scm), std::ref(*r3));
          t1.join();
          t2.join();
          t3.join();
          r.join_delete(r2);
          r.join_delete(r3);
        } else {
          Rower* r2 = dynamic_cast<Rower*>(r.clone());
          int rowCount = nrows();
          Schema* scm = new Schema(this->get_schema());
          std::thread t1(&DataFrame::pTraversal, std::ref(*this), 0, std::cref(rowCount)/2, std::ref(scm), std::ref(r));
          std::thread t2(&DataFrame::pTraversal, std::ref(*this), std::cref(rowCount)/2, std::cref(rowCount), std::ref(scm), std::ref(*r2));
          t1.join();
          t2.join();
          //wasting one good thread, why - forgetting the main thread man
          r.join_delete(r2);
        }
    }
    /*
    * Helper method for pmap that given a start, stop, rower, and schema iterates over the given range of df rows
    */ 
    void pTraversal(size_t start, size_t end, Schema* scm, Rower& r) {
        Row* row = new Row(*scm);
        for(int i = start; i < end; i++) {
            this->fill_row(i, *row);
            row->set_idx(i);
            r.accept(*row);
        }
    }

  
    /** Create a new dataframe, constructed from rows for which the given Rower
      * returned true from its accept method. */
    DataFrame* filter(Rower& r) {
      DataFrame* df = new DataFrame(*this);
      //create a new row.
      Row* row = new Row(*this->schema_);

      //apply the rower to each row.
      for (size_t i = 0; i < this->schema_->length(); i++) {
        this->fill_row(i, *row);
        if(r.accept(*row)) {
          df->add_row(*row);
        }
      }

      delete row;
      return df;
    }
  
    /** Print the dataframe in SoR format to standard output. */
    void print();

    /**
     * Equality check
    */
    bool equals(Object  * other) {
      if (other == this) return true;
        DataFrame* x = dynamic_cast<DataFrame*>(other);
        if (x == nullptr) return false;
        if (this->ncols() != x->ncols()) return false;
        if (this->nrows() != x->nrows()) return false;
        for (size_t i = 0; i < this->ncols(); i++) {
            if (!(this->cols_->get(i)->equals(x->cols_->get(i)))) { 
                return false; 
            }
        }
        return true;
    }

    static DataFrame* fromArray(Key* key, KVStore* kv, size_t length, double* vals) {
      
    }
};