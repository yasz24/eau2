#pragma once
#include "../utils/set.h"
#include "../dataframe/visitor.h"
/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Reader {
public:
  Set& uSet; // set of collaborator 
  Set& pSet; // set of projects of collaborators
  Set newProjects;  // newly tagged collaborator projects

  ProjectsTagger(Set& uSet, Set& pSet, DistributedDataFrame* proj):
    uSet(uSet), pSet(pSet), newProjects(proj) {}

  /** The data frame must have at least two integer columns. The newProject
   * set keeps track of projects that were newly tagged (they will have to
   * be communicated to other nodes). */
  bool visit(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (uSet.test(uid)) 
      if (!pSet.test(pid)) {
    	pSet.set(pid);
        newProjects.set(pid);
      }
    return false;
  }
};