#pragma once
#include "string.h"
#include "../store/key.h"
#include "../object.h"


class KeyBuff : public Object {                                                  
  public:                                                                        
  Key* orig_; // external                                                        
  StrBuff buf_;                                                                  
                                                                                 
  KeyBuff(Key* orig) {
    orig_ = orig;
    buf_ = StrBuff();
    buf_.c(*orig->name_);
  }                               
                                                                                 
  KeyBuff& c(String &s) { buf_.c(s); return *this;  }                            
  KeyBuff& c(size_t v) { buf_.c(v); return *this; }                              
  KeyBuff& c(const char* v) { buf_.c(v); return *this; }                         
                                                                                 
  Key* get() {                                                                   
    String* s = buf_.get();                                                      
    buf_.c(orig_->c_str());                                                      
    Key* k = new Key(s->steal(), orig_->node());                                 
    delete s;                                                                    
    return k;                                                                    
  }                                                                              
}; // KeyBuff