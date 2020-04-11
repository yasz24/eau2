#pragma once
#include "../utils/set.h"
#include "application.h"
//new stuff
#include "setUpdater.h"
#include "setWriter.h"
#include "projectsTagger.h"
#include "usersTagger.h"
/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
  int DEGREES = 4;  // How many degrees of separation form linus?
  int LINUS = 4967;   // The uid of Linus (offset in the user df)
  const char* PROJ = "datasets/projects.ltgt";
  const char* USER = "datasets/users.ltgt";
  const char* COMM = "datasets/commits.ltgt";
  DataFrame* projects; //  pid x project name
  DataFrame* users;  // uid x user name
  DataFrame* commits;  // pid x uid x uid 
  Set* uSet; // Linus' collaborators
  Set* pSet; // projects of collaborators

  Linus(size_t idx, NetworkIfc& net): Application(idx, net) {}

  /** Compute DEGREES of Linus.  */
  void run_() override {
    readInput();
    for (size_t i = 0; i < DEGREES; i++) step(i);
  }

  /** Node 0 reads three files, cointainng projects, users and commits, and
   *  creates thre dataframes. All other nodes wait and load the three
   *  dataframes. Once we know the size of users and projects, we create
   *  sets of each (uSet and pSet). We also output a data frame with a the
   *  'tagged' users. At this point the dataframe consists of only
   *  Linus. **/
  void readInput() {
    Key pK("projs");
    Key uK("usrs");
    Key cK("comts");
    if (index == 0) {
      pln("Reading...");
      projects = DataFrame::fromFile(PROJ, pK.clone(), &kv);
      p("    ").p(projects->nrows()).pln(" projects");
      users = DataFrame::fromFile(USER, uK.clone(), &kv);
      p("    ").p(users->nrows()).pln(" users");
      commits = DataFrame::fromFile(COMM, cK.clone(), &kv);
       p("    ").p(commits->nrows()).pln(" commits");
       // This dataframe contains the id of Linus.
       delete DataFrame::fromScalarInt(new Key("users-0-0"), &kv, LINUS);
    } else {
       projects = dynamic_cast<DataFrame*>(kv.waitAndGet(pK));
       users = dynamic_cast<DataFrame*>(kv.waitAndGet(uK));
       commits = dynamic_cast<DataFrame*>(kv.waitAndGet(cK));
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
    DataFrame* newUsers = dynamic_cast<DataFrame*>(kv.waitAndGet(uK));    
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
      for (size_t i = 1; i < arg.num_nodes; ++i) {
	Key nK(StrBuff(name).c(stage).c("-").c(i).get());
	DataFrame* delta = dynamic_cast<DataFrame*>(kv.waitAndGet(nK));
	p("    received delta of ").p(delta->nrows())
	  .p(" elements from node ").pln(i);
	SetUpdater upd(set);
	delta->map(upd);
	delete delta;
      }
      p("    storing ").p(set.size()).pln(" merged elements");
      SetWriter writer(set);
      Key k(StrBuff(name).c(stage).c("-0").get());
      delete DataFrame::fromVisitor(&k, &kv, "I", writer);
    } else {
      p("    sending ").p(set.size()).pln(" elements to master node");
      SetWriter writer(set);
      Key k(StrBuff(name).c(stage).c("-").c(index).get());
      delete DataFrame::fromVisitor(&k, &kv, "I", writer);
      Key mK(StrBuff(name).c(stage).c("-0").get());
      DataFrame* merged = dynamic_cast<DataFrame*>(kv.waitAndGet(mK));
      p("    receiving ").p(merged->nrows()).pln(" merged elements");
      SetUpdater upd(set);
      merged->map(upd);
      delete merged;
    }
  }
}; // Linus