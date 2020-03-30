#include "kvstore.h"

class KDStore : public KVStore {

    KDStore(size_t num_nodes, size_t this_node) : KVStore(num_nodes, this_node) {};
};