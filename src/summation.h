#pragma once
#include "rower.h"
#include "row.h"
#include "dataframe.h"

class Summation : public Rower {  

public:
 DataFrame* df_;
 int sum_;
 Summation(DataFrame* df) {
     df_ = df;
     sum_ = 0;
 }
 size_t to_sum=1;  // these are for convenience
 bool accept(Row& r) {
    sum_ += r.get_int(to_sum);
   return true;
}
void join_delete(Rower* other) {
    Summation * other_summer = dynamic_cast<Summation*>(other);
    if(other_summer == nullptr) return;
    sum_ += other_summer->sum_;
    delete other_summer;
}

virtual Object* clone() { 
    return new Summation(df_);
}

};