# notes on scaling elasticsearch

- https://www.elastic.co/guide/en/elasticsearch/guide/current/index.html
- https://www.elastic.co/guide/en/elasticsearch/guide/master/_scale_horizontally.html
- https://www.elastic.co/guide/en/elasticsearch/guide/current/scale.html

## verticle scale vs. horizontal scale

- verticle: increase the power of each server
- horizontal: increase the amount of servers

## components

- cluster
    - one or more nodes
    - one master node
    - reorganized by itself in case of scaling
- master node
    - managing cluster-wide changes
        - create/delete index
        - add/remove node
    - does not need to be involved in document-level changes
        - not necessary a bottleneck
    - every node can be the master
- node
    - every node knows where each document lives
    - forwards request if necessary
    - request handling transparently managed by elasticsearch
- index
    - logical namespace for data
    - points to one or more shards
- shards (primary shard)
    - single instance of lucene index
    - handles a part of an elasticsearch index
    - documents are stored in shards
    - shards are allocated to nodes
    - shards are balanced by elasticsearch
    - number shards per index is fixed after creation! (default: 5)
    - replica shard: copy of primary shard, number not fixed

## capacity planning

- little overallocation is ok but not to much
- find out the capacity of a single shard:
    1. cluster on single server with produrction hardware
    2. index with one primary shard and no replica
    3. real documents 
    4. real queries / aggregations 
    5. push until it breaks (responses get to slow. What is slow?)
- total amount of data + some extra diveded by capacity of a single shard makes the number of primary shards
- but firstly try to optimize how elasticsearch is used
    - inefficient queries?
    - not enough RAM?
    - left swap enabled?

## reindexing

- should be the last option
- https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html
    
## replica

- main purpose for failover
- can serve read requests
- balancing load
    - e.g. 2 shard on 3 nodes, 2 replica, evenly distributed
    
## multiple indices

- "Searching 1 index of 50 shards is exactly equivalent to searching 50 indices with 1 shard each: both search requests hit 50 shards."
- add capacity on the fly, using alias and zero downtime, transparent
    - one alias for search one for indexing
    - add another index 
    - remove alias for indexing from the first index
    - add alias for indexing and searching to the second index
- time-based data
- index templates, basically settings for automatic new index creation
- retiring data
    - old data may not relevant anymore (time-based)
        - delete
        - optimize
        - close
        - archiving
    - tell elasticsearch to put relevant data to a stronger machine or not to relevant data to a weaker machine by box_type and index settings
    