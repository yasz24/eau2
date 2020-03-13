## Introduction:

The eau2 system is a distributed key value store that allows us to build complex data structures on top of a framework of interconnected computers that increases overall capacity of our system and computation rates.
 
## Architecture:

The architecture of the system is three-tiered. The bottom layer is a KV store,
distributed across several nodes. The networking and concurrency components of the KV store also live here and are hidden from the end user. The next level leverages the code below to provide abstractions for a wide range of data-structures that are represented in some way across the distributed in some way across the KV store. The topmost layer is the application layer where users of the system write code that uses these data-structures as if they were one single entity. In doing so, the eau2 systems provides the ability for large systems to operate with data-structures that would otherwise be too large to hold in memory on one single machine. 

## Implementation:

```
class KVStore {
//fields.

//methods:
void put(Key k, Value v);
Value get(Key k);
Value getAndWait(Key k);  // blocking.
}

class Key {
String name;
size_t node;
}

class Value {
char* data;
}
```


Let’s consider representing a dataframe with 100 rows in it on the eau2 system that has a single home node and 10 auxiliary nodes. 

The home node creates a dataframe object containing chunks of 10 rows at a time as such.
```
Dataframe* df1 = new Dataframe();
df1->add_row(r1);
df1->add_row(r2);
.
.
.
df1->add_row(r10);

---
Dataframe* df2 = new Dataframe();
df2->add_row(r11);
df2->add_row(r12);
.
.
.
df2->add_row(r20);

---

.
.
.
---
Dataframe* df10 = new Dataframe();
df10->add_row(r91);
df10->add_row(r92);
.
.
.
df10->add_row(r100);
```

It then creates unique keys to assign to each of the dataframe chunks.

```
Key first(“chunk 1”, 1)
Key second(“chunk 2”, 2)
.
.
.
Key tenth(“chunk 10”, 10)

```

The home node then simply puts the serialized data frames into its KV store which then dispatches the KV pair to the appropriate node to be stored.

```
kv.put(first, df1.serialize()); //stored on node1
kv.put(second, df2.serialize()); //stored on node2
.
.
.
kv.put(tenth, df10.serialize());
```

And finally stores the data frame as a heap-allocated array of keys in its memory.

```
Key* dataframe = new Key[10];
dataframe[0] = &first;
dataframe[1] = &second;
.
.
.
dataframe[9] = &tenth;
```

Here we looked at storing a dataframe with 100 rows, distributed across 10 nodes, however, this can be scaled up to store a dataframe with any num_rows, and distributed across any num_nodes.

And that’s it, We have implemented a distributed dataframe using the eau2 system!

## Use cases:
Two potential use cases for our distributed dataframe are for increased capacity, and redundancy. For increased capacity, this would allow our clients to store more information than one single server can contain and create a massive datastore with seamless integration that feels to the end user like all the data is in the same place.

On the other end of the spectrum, we could use a distributed dataframe for backup control. If one of the nodes of the system goes down and each node had the same information, then it is still easily accessible. Someone could potentially implement the RAFT or PAXOS protocols in the application layer for further protection.

## Open questions: 
We are currently at a dearth of understanding about future milestones, and hence have no questions at the point.

## Status:
Currently we have brought our existing dataframe implementation up to speed and combined it with a C++ Sorer representation provided by github user euhlmann (https://github.ccs.neu.edu/euhlmann/CS4500-A1-part1). 