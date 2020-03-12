#pragma once
#include "rower.h"
#include "dataframe.h"
#include "row.h"
#include "printFielder.h"

class DataFrame;

class PrintRower : public Rower {  

public:
 DataFrame* df_;
 StrBuff* strBuff; 
 PrintRower(DataFrame* df) {
     df_ = df;
     strBuff = new StrBuff();
 }

 bool accept(Row& r) {
    r.visit(r.get_idx(), *(new PrintFielder()));
    return true;
}

void join_delete(Rower* other) {

}

virtual Object* clone() { 
    return new PrintRower(df_);
}

};