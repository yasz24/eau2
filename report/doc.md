## Introduction:

The eau2 system is a distributed key value store that allows us to build complex data structures on top of a framework of interconnected computers that increases overall capacity of our system and computation rates. This is based around the interaction of two main APIs: 
1) A client-facing interface that shares a lot of similarities with our previous dataframe implementation. To put it simply, the two should work almost identically
2) A private API that uses a distributed form of the column and array data structures that are able to spread data chunked by column across multiple nodes in the same network to increase overall capacity. 
 
## Architecture:

The architecture of the system is three-tiered.

The bottom layer is a KV store, run on each of the networked nodes containing an underlying map for key to value. Each key passed to the KVStore includes Node information so the request can be routed to the correct storage location. 
This is also where the networking and concurrency information live to be as far away from the end user as possible and really only necessary at the KVStore level since everything else will be built under the premise there is only one KVStore to connect to - and it will do the rest. 

The next level is our original bottom platform of Arrays, Columns, and Queues. Each adapted to now store data in a KVStore, updating the appropriate networked-node to send the next chunk of data to and how to pull data effectively from the KVStore and network architecture. 

The topmost layer is the application layer where users of the system write code that uses these data-structures, particularly our now-distributed dataframe, as if it was completely stored on a single machine. In doing so, the eau2 systems provides the ability for large systems to operate with data-structures of a size that would otherwise be too large to hold in memory on one single machine. 

## Implementation:

```
class KVStore {
//fields.

//methods:
void put(Key k, Value v);
Value get(Key k);
Value getAndWait(Key k);  // blocking.
}
```
KVStore is where the majority of the all of the node delegation networking occurs. Each of the put, get, and getAndWait methods given a key will either return the value if it is on the node of this KVStore, or they will have enough information to make a network request to the appropriate node and get the value

```
class Key {
String name;
size_t node;
}

class Value {
char* data;
}
```
Keys include a specific name that is unique to class, stored values, whatever helps identify it, and a node number so the KVStore knows how to dispatch. 
Value is expected to contain serialized data that will be stored in the KVStore
```
class DistributedArray {
    KVStore* kv_;
    Array* chunkArray_;
    Array* keys;
    size_t uid_ = rand();
    size_t chunkSize, chunkCount, itemCount, curNode, totalNodes;
}
```
If KVStores are the airplanes shuttling data back and forth, the distributedArrays are mission control. They keep track of how much data goes in each "chunk" that is stored on a node before starting to store information on the next node in the system. They also keep track of all the keys associated with their data and randomly generate a UID to minimize the risk of storing data with the same key as another distributedArray. itemCount, ChunkCount, and chunkArray are all to further help the Array delegate data effectively and with a minimum amount of calls to the KVStore assuming every time we reach out to the KVStore it'll require a network request.

```
class DistributedColumn : public Column {
    KVStore* kv_;
    DistributedArray* val_;
}
```
DistributedColumn is essentially a wrapper over the DistributedArrays that add more metadata for our Dataframe implementation (Columntype, as_int, as_bool) that allow all DistributedColumns to inherit from the same baseclass with all the same functionality and allow for polymorphism in terms of each type of DistributedColumn (int, bool, float) having the same getters and setters.

```
class DistributedDataFrame() {
    Schema* schema_;
    Array* cols_;
    KVStore* kv_;
}
```
Since all of our delegation and networking calls are done on much lower levels - it allows our DistributedDataFrame to be virtually indistinguishable from our previous DataFrame model. Besides the addition of a KVStore in the constructor which can then be passed down appropriately, all the accessor methods of getting rows, columns, or specific values function very much the same as before but call upon DistributedColumns as opposed to regular columns. This is the layer our clients should be seeing.

## Use cases:

Perhaps the biggest use-case for our distributed KVStore is with the aformentioned Dataframe implementation. This class gives clients access to large-scale data storage with essentially limitless capability depending on how many nodes they add to the KVStore network.

Let’s consider representing a dataframe with 100 rows in it on the eau2 system that has a single home node and 10 auxiliary nodes. 

The home node creates a dataframe object and begins to add previously defined row objects.
```
KVStore(num_nodes: 10, this_node: 0)
.
.
.
KVStore(num_nodes: 10, this_node: 10)
//these are added on each node in the system

Dataframe* df1 = new Dataframe(schema, kvstore0);
df1->add_row(r1);
df1->add_row(r2);
.
df1->add_row(r100000000);
---
```
Once the KVStores are up and running on each of the individual nodes, passing the first one to the Dataframe constructor will link them to the Dataframe in a way that the end user can continue to use the Dataframe in the ways they're used to but with in this case up to 10x the previous capacity.

This example dataframe has 100,000,000 rows distributed across 10 nodes...but this is completely arbitrary and we could have simply kept going.

Another use case for the more technical users of this interface who aren't happy with our dataframe storage solution could elect to build their own or any further extension of the DistributedArray.

```
IntDistributedArray* ida = new IntDistributedArray(KVStore: kv, chunkSize: chunk);
```
Given another KVStore like earlier, you can also tweak the exact parameters that change how large data is allowed to get before storing on the next node in the sequence
```
void pushBack(int val) {
    //initialize first key
    if(itemCount_ % chunkSize_ == 0 && itemCount_ != 0) { //if current chunk is full..
        storeChunk(); //stores the previous chunk values in the kv_store
    } else {
        chunkArray_->pushBack(val);
    }
    itemCount_ +=1;
}; //add o to end of array
```
By changing the chunkSize you can minimize the number of network calls if that is something your application requires or for further redundancy you can reduce chunkSize to make sure data is constantly being stored in the other nodes.

## Open questions: 
We're beginning work on the Networking layer that will finally put our distributedArray and dataframe structures to the test. While we did meet with Jan to help clear up some stuff we're still stuck kind of wrapping our heads around the idea of how to simulate this on one machine. It does help to have an idea where this is all headed though - the inclusion of every milestone has really helped us out.

## Status:
For MS1:
Currently we have brought our existing dataframe implementation up to speed and combined it with a C++ Sorer representation provided by github user euhlmann (https://github.ccs.neu.edu/euhlmann/CS4500-A1-part1). We are able to successfully read .sor files into our dp using the main.cpp file located in the sorer directory.  


03/29/20:
To put it simply - coronavirus. We've been doing our best to keep up with the workload and adjust to this system of working remote and zoom calling but it definitely has taken a long time to get used to since that really wasn't our style prior to moving completely online. Milestone 2 took significantly longer than expected but we're happy with the result, re-evaluating extra baggage in the codebase. For this milestone our focus has been on the distributed aspect involving new code in:
* utils/distributedArray.h
* dataframe/distributedColumn.h
* dataframe/distributedDataframe.h
* store/
* updated serialization all around with tests in serialize/main.cpp and serialize/testSerialize.cpp  


While we did encounter a malloc error when actually running the application code for MS2 (testApplication() in tests/testSerialize.cpp) we believe we have all the pieces to make the code snippet run, and it is only a case of debugging.