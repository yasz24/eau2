#pragma once
#include "../utils/set.h"
#include "application.h"
#include "../network/network_ip.h"
//new stuff
#include "setUpdater.h"
#include "setWriter.h"
#include "projectsTagger.h"
#include "usersTagger.h"
#include "../sorer/parser.h"
/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
  int DEGREES = 4;  // How many degrees of separation form linus?
  int LINUS = 4967;   // The uid of Linus (offset in the user df)
  const char* PROJ = "../src/application/projects.ltgt";
  const char* USER = "../src/application/users.ltgt";
  const char* COMM = "../src/application/commits.ltgt";
  DistributedDataFrame* projects; //  pid x project name
  DistributedDataFrame* users;  // uid x user name
  DistributedDataFrame* commits;  // pid x uid x uid 
  Set* uSet; // Linus' collaborators
  Set* pSet; // projects of collaborators

    //actual constructor
    Linus(size_t idx, NetworkIP* net): Application(idx, net) {}


  /** Compute DEGREES of Linus.  */
  void run_() override {
    readInput();
    pln("read input...");
    for (size_t i = 0; i < DEGREES; i++) step(i);
  }

  /**
   * helper function to generate DistributedDataFrames from files: 
   * a patchwork solution to deal with the circular
   * depenency issue of having a parser class that generates a DistributedDataframe and a DistributedDataFrame method
   * that returns a dataframe using that parser method. 
   * 
   * Some other ideas included changing parser.h and distributedDataframe.h to have .cpp files with implementation
   * details but we believe that increase in complexity wasn't offset by the benefit of not having this helper
   * 
   */ 
  DistributedDataFrame* getddfFromFile(const char* filename, Key* key, KVStore* kv) {
    std::cout<<"filename: "<<filename<<"\n";
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
    printf("ERROR! Failed to open file\n");
    return nullptr;
    }
    fseek(file, 0, SEEK_END);
    size_t file_size = ftell(file);
    fseek(file, 0, SEEK_SET);
    //set argument defaults
    SorParser parser{file, (size_t)0, file_size, file_size};

    parser.guessSchema();
    parser.parseFile();
    DistributedDataFrame* df = parser.getDistributedDataFrame(kv);
    Value* val = new Value(df->serialize(), (size_t)0);
    kv->put(key, val);
    return df;
}

  /** Node 0 reads three files, containing projects, users and commits, and
   *  creates three dataframes. All other nodes wait and load the three
   *  dataframes. Once we know the size of users and projects, we create
   *  sets of each (uSet and pSet). We also output a data frame with the
   *  'tagged' users. At this point the dataframe consists of only
   *  Linus. **/
  void readInput() {
    Key pK("projs", this_node_);
    Key uK("usrs", this_node_);
    Key cK("comts", this_node_);
    if (this_node_ == 0) {
      projects = getddfFromFile(PROJ, pK.clone(), kv_);
      p("    ").p(projects->nrows()).pln(" projects");
      users = getddfFromFile(USER, uK.clone(), kv_);
      p("    ").p(users->nrows()).pln(" users");
      commits = getddfFromFile(COMM, cK.clone(), kv_);
       p("    ").p(commits->nrows()).pln(" commits");
       // This dataframe contains the id of Linus.
       delete DistributedDataFrame::fromScalar(new Key("users-0-0", this_node_), kv_, LINUS);
    } else {
       projects = dynamic_cast<DistributedDataFrame*>(kv_->waitAndget(&pK));
       users = dynamic_cast<DistributedDataFrame*>(kv_->waitAndget(&uK));
       commits = dynamic_cast<DistributedDataFrame*>(kv_->waitAndget(&cK));
    }
    uSet = new Set(users);
    pSet = new Set(projects);
 }

 /** Performs a step of the linus calculation. It operates over the three
  *  datafrrames (projects, users, commits), the sets of tagged users and
  *  projects, and the users added in the previous round. */
  void step(int stage) {
    p("Stage ").pln(stage);
    // Key of the shape: users-stage-0
    Key uK(StrBuff("users-").c(stage).c("-0").get());
    // A df with all the users added on the previous round
    DistributedDataFrame* newUsers = new DistributedDataFrame(kv_->waitAndget(&uK)->data, kv_);    
    pln(newUsers->schema_->val_);
    Set delta(users);
    SetUpdater upd(delta);
    newUsers->map(upd); // all of the new users are copied to delta.
    delete newUsers;
    ProjectsTagger ptagger(delta, *pSet, projects);
    commits->local_map(ptagger); // marking all projects touched by delta
    merge(ptagger.newProjects, "projects-", stage);
    pSet->union_(ptagger.newProjects); // 
    UsersTagger utagger(ptagger.newProjects, *uSet, users);
    commits->local_map(utagger);
    merge(utagger.newUsers, "users-", stage + 1);
    uSet->union_(utagger.newUsers);
    p("    after stage ").p(stage).pln(":");
    p("        tagged projects: ").pln(pSet->size());
    p("        tagged users: ").pln(uSet->size());
  }

  /** Gather updates to the given set from all the nodes in the systems.
   * The union of those updates is then published as dataframe.  The key
   * used for the otuput is of the form "name-stage-0" where name is either
   * 'users' or 'projects', stage is the degree of separation being
   * computed.
   */ 
  void merge(Set& set, char const* name, int stage) {
    if (this_node() == 0) {
      for (size_t i = 1; i < kv_->num_nodes_; ++i) {
        Key* nK = new Key(StrBuff(name).c(stage).c("-").c(i).get());
        DistributedDataFrame* delta = new DistributedDataFrame(kv_->waitAndget(nK)->data, kv_);
        p("    received delta of ").p(delta->nrows())
          .p(" elements from node ").pln(i);
        SetUpdater upd(set);
        delta->map(upd);
        delete delta;
      }
      p("    storing ").p(set.size()).pln(" merged elements");
      SetWriter* writer = new SetWriter(set);
      Key k(StrBuff(name).c(stage).c("-0").get());
      delete DistributedDataFrame::fromVisitor(&k, kv_, "I", writer);
    } else {
      p("    sending ").p(set.size()).pln(" elements to master node");
      SetWriter* writer = new SetWriter(set);
      Key k(StrBuff(name).c(stage).c("-").c(this_node()).get()); // lives on node 0?
      delete DistributedDataFrame::fromVisitor(&k, kv_, "I", writer); // where the send "happens"
      Key mK(StrBuff(name).c(stage).c("-0").get());
      DistributedDataFrame* merged = new DistributedDataFrame(kv_->waitAndget(&mK)->data, kv_);
      p("    receiving ").p(merged->nrows()).pln(" merged elements");
      SetUpdater upd(set);
      merged->map(upd);
      delete merged;
    }
  }
}; // Linus