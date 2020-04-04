#pragma once
#include "../utils/string.h"
#include "../dataframe/visitor.h"
#include "../helper.h"
#include <stdio.h>
#include <stdlib.h>
#include <ostream>

class FileReader : public Writer {
public:
  /** Reads next word and stores it in the row. Actually read the word.
      While reading the word, we may have to re-fill the buffer  */
    void visit(Row & r) override {
       assert(i_ < end_);
        assert(! isspace(buf_[i_]));
        size_t wStart = i_;
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) { ++i_;  break; }
                i_ = wStart;
                wStart = 0;
                fillBuffer_();
            }
            if (isspace(buf_[i_]))  break;
            ++i_;
        }
        buf_[i_] = 0;
        String* word = new String(buf_ + wStart, i_ - wStart);
        std::cout<<"word found: "<<word->c_str()<<"\n";
        r.set(0, word);
        ++i_;
        skipWhitespace_();
    }
 
    /** Returns true when there are no more words to read.  There is nothing
       more to read if we are at the end of the buffer and the file has
       all been read.     */
    bool done() override {
        return (i_ >= end_) && feof(file_);  
    }
 
    /** Creates the reader and opens the file for reading.  */
    FileReader(char* fileName) {
        Sys print = Sys();
        std::cout<<"filename: "<<fileName<<"\n";
        file_ = fopen(fileName, "r");
        print.exit_if_not(file_ != nullptr, "Cannot open file");
        buf_ = new char[BUFSIZE + 1]; //  null terminator
        fillBuffer_();
        skipWhitespace_();
    }
 
    static const size_t BUFSIZE = 1024;
 
    /** Reads more data from the file. */
    void fillBuffer_() {
        size_t start = 0;
        // compact unprocessed stream
        if (i_ != end_) {
            start = end_ - i_;
            memcpy(buf_, buf_ + i_, start);
        }
        // read more contents
        end_ = start + fread(buf_+start, sizeof(char), BUFSIZE - start, file_);
        i_ = start;
    }
 
    /** Skips spaces.  Note that this may need to fill the buffer if the
        last character of the buffer is space itself.  */
    void skipWhitespace_() {
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) return;
                fillBuffer_();
            }
            // if the current character is not whitespace, we are done
            if (!isspace(buf_[i_]))
                return;
            // otherwise skip it
            ++i_;
        }
    }
 
    char * buf_;
    size_t end_ = 0;
    size_t i_ = 0;
    FILE * file_;
};