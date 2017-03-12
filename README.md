# CqlInject

[![Build Status](https://travis-ci.org/strapdata/cqlinject.svg)](https://travis-ci.org/strapdata/cqlinject)

CqlInject is a simple data loader for apache cassandra 2.2+ :
* Insert the result of a JDBC query in a cassandra table (1).
* Insert the result of a CQL query from a cassandra cluster (1).
* Insert from a CSV file (1).
* Insert an Elasticsearch bulk JSON file (see [Elasticsearch bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html))(2)

(1) automatically creates or updates the destination table schema.
(2) destination table schema should match the JSON input record format.

WARNING: CqlInject runs on a single JVM, it may not fit to move huge volume of data.

# Installation

## Building from source

* CqlInject uses [Maven](http://maven.apache.org) for its build system. Simply run the `mvn clean package -Dmaven.test.skip=true` command in the cloned directory. 
The distribution will be created under *target/releases*.

## Tarball Installation

* Install Java version 8 (check version with `java -version`). 
* Download CqlInject tarball from [CqlInject repository](https://github.com/vroyer/cqlinject/releases) and extract files in your installation directory.
* Add CqlInject into your PATH environnement variable `PATH="$PATH:<installation_dir>/bin"`.

# Usage examples

## CqlInject from a cassandra cluster

Execute a CQL query and write the result in the destination cluster.keyspace.table. If the destination table does not exists, CqlInject create it.
```
cqlinject -h localhost -ks music -cf playlists -pk "id,song_order" -ptlen 1 cql 
-sh host2 
"SELECT * FROM playlists"
```

## CqlInject from prestoDB 

[prestoDB](https://prestodb.io/) is a distributed SQL engine allowing to queries various systems including Hadoop, Cassandra, Mongodb, Redis, Kafka. 
With to [prestodDb JDBC client](https://prestodb.io/docs/current/installation/jdbc.html) driver, you can write the result of a prestodDB query 
in a dynamically created (or updated) cassandra table. In the following exemple, it joins two tables and write the result in the first table, adding new columns.

```
cqlinject -h localhost -ks twitter -cf tweet -pk "msg_id" jdbc 
--jdbc.url "jdbc:presto://localhost:8080" 
--jdbc.user bob -jdbc.password bob 
"SELECT t.msg_id, u.country FROM twitter t INNER JOIN users u ON (t.user = u.user)"
```

## CqlInject from elasticsearch

Bound with the elasticsearch JDBC driver [sql4es ](https://github.com/Anchormen/sql4es), CqlInject can move data from elasticsearch to cassandra.

```
cqlinject -h localhost -ks twitter -cf tweet -pk "_id" jdbc 
--jdbc.url "jdbc:sql4es://node1:9200/twitter?cluster_name=MyCluster" 
"SELECT * FROM twitter WHERE size > 100"
```

## CQL Inject a CSV file

Insert a CSV File :
* First line is column names separated by your separator.
* Second line is MessageFormat pattern for each field separated by your separator.

```
cqlinject -ks lastfm -pk "_id" -cf profile csv --separator '\t' userid-profile.tsv
```

CSV File content example :

```
_id     gender  age     country registred
{0}     {0}     {0,number}      {0}     {0,date,MMM dd, yyyy}
user_000001     m       0       Japan   Feb 23, 2006
user_000002     f       0       Peru    Feb 24, 2006
user_000003     m       22      United States   Oct 30, 2005
user_000004     f                       Apr 26, 2006
user_000005     m               Bulgaria        Jun 29, 2006
user_000006             24      Russian Federation      May 18, 2006
user_000007     f               United States   Jan 22, 2006
user_000008     m       23      Slovakia        Sep 28, 2006
user_000009     f       19      United States   Jan 13, 2007
```

## CqlInject JSON bulk load

Elasticsearch provide a [bluk API](https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html) to load JSON files into elasticsearch. 
CqlInject reads this JSON file format and build an INSERT INTO \<index\>.\<type\> JSON \<doc\>' see [Cassandra JSON support](http://www.datastax.com/dev/blog/whats-new-in-cassandra-2-2-json-support). If your document _id is a string representation of a JSON array, 
each array element are mapped to primary key columns of the cassandra destination table (so the destination table must exists before injecting).
```
cqlinject -h localhost json shakespeare.json
```

# License

```
This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2017, Strapdata (contact@strapdata.com).

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
