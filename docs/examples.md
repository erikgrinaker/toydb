# SQL Usage Examples

The following examples demonstrate some of toyDB's SQL features. For more details, see the
[SQL reference](sql.md).

- [Setup](#setup)
- [Creating Tables and Data](#creating-tables-and-data)
- [Constraints and Referential Integrity](#constraints-and-referential-integrity)
- [Basic SQL Queries](#basic-sql-queries)
- [Expressions](#expressions)
- [Joins](#joins)
- [Explain](#explain)
- [Aggregates](#aggregates)
- [Transactions](#transactions)
- [Time-Travel Queries](#time-travel-queries)

## Setup

To start a five-node cluster on the local machine (requires a working
[Rust compiler](https://www.rust-lang.org/tools/install)), run:

```
$ (cd clusters/local && ./run.sh)
toydb-b 19:06:28 [ INFO] Listening on 0.0.0.0:9602 (SQL) and 0.0.0.0:9702 (Raft)
toydb-b 19:06:28 [ERROR] Failed connecting to Raft peer 127.0.0.1:9705: Connection refused
toydb-e 19:06:28 [ INFO] Listening on 0.0.0.0:9605 (SQL) and 0.0.0.0:9705 (Raft)
[...]
toydb-e 19:06:29 [ INFO] Voting for toydb-d in term 1 election
toydb-c 19:06:29 [ INFO] Voting for toydb-d in term 1 election
toydb-d 19:06:29 [ INFO] Won election for term 1, becoming leader
```

In a separate terminal, start a `toysql` client and check the server status:

```
$ cargo run --release --bin toysql
Connected to toyDB node "toydb-e". Enter !help for instructions.
toydb> !status

Server:    toydb-e (leader toydb-d in term 1 with 5 nodes)
Raft log:  1 committed, 0 applied, 0.000 MB (hybrid storage)
Node logs: toydb-a:1 toydb-b:1 toydb-c:1 toydb-d:1 toydb-e:1
SQL txns:  0 active, 0 total (memory storage)
```

The cluster is shut down by pressing Ctrl-C. Data is saved under `clusters/local/toydb-?/data/`,
delete the contents to start over.

## Creating Tables and Data

As a basis for later examples, we'll create a small movie database. The following SQL statements
can be pasted into `toysql`:

```sql
CREATE TABLE genres (
    id INTEGER PRIMARY KEY,
    name STRING NOT NULL
);
INSERT INTO genres VALUES
    (1, 'Science Fiction'),
    (2, 'Action'),
    (3, 'Drama'),
    (4, 'Comedy');

CREATE TABLE studios (
    id INTEGER PRIMARY KEY,
    name STRING NOT NULL
);
INSERT INTO studios VALUES
    (1, 'Mosfilm'),
    (2, 'Lionsgate'),
    (3, 'StudioCanal'),
    (4, 'Warner Bros'),
    (5, 'Focus Features');

CREATE TABLE movies (
    id INTEGER PRIMARY KEY,
    title STRING NOT NULL,
    studio_id INTEGER NOT NULL INDEX REFERENCES studios,
    genre_id INTEGER NOT NULL INDEX REFERENCES genres,
    released INTEGER NOT NULL,
    rating FLOAT
);
INSERT INTO movies VALUES
    (1,  'Stalker',             1, 1, 1979, 8.2),
    (2,  'Sicario',             2, 2, 2015, 7.6),
    (3,  'Primer',              3, 1, 2004, 6.9),
    (4,  'Heat',                4, 2, 1995, 8.2),
    (5,  'The Fountain',        4, 1, 2006, 7.2),
    (6,  'Solaris',             1, 1, 1972, 8.1),
    (7,  'Gravity',             4, 1, 2013, 7.7),
    (8,  '21 Grams',            5, 3, 2003, 7.7),
    (9,  'Birdman',             4, 4, 2014, 7.7),
    (10, 'Inception',           4, 1, 2010, 8.8),
    (11, 'Lost in Translation', 5, 4, 2003, 7.7),
    (12, 'Eternal Sunshine of the Spotless Mind', 5, 3, 2004, 8.3);
```

toyDB supports some basic datatypes, as well as primary keys, foreign keys, and column indexes.
For more information on these, see the [SQL reference](sql.md). Schema changes such as
`ALTER TABLE` are not supported, only `CREATE TABLE` and `DROP TABLE`.

The tables can be inspected via the `!tables` and `!table` commands:

```sql
toydb> !tables
genres
movies
studios

toydb> !table genres
CREATE TABLE genres (
  id INTEGER PRIMARY KEY,
  name STRING NOT NULL
)
```

## Constraints and Referential Integrity

Schemas enforce referential integrity and other constraints:

```sql
toydb> DROP TABLE studios;
Error: Table studios is referenced by table movies column studio_id

toydb> DELETE FROM studios WHERE id = 1;
Error: Primary key 1 is referenced by table movies column studio_id

toydb> UPDATE movies SET id = 1;
Error: Primary key 1 already exists for table movies

toydb> INSERT INTO movies VALUES (13, 'Nebraska', 6, 3, 2013, 7.7);
Error: Referenced primary key 6 in table studios does not exist

toydb> INSERT INTO movies VALUES (13, 'Nebraska', NULL, 3, 2013, 7.7);
Error: NULL value not allowed for column studio_id

toydb> INSERT INTO movies VALUES (13, 'Nebraska', 'Unknown', 3, 2013, 7.7);
Error: Invalid datatype STRING for INTEGER column studio_id
```

## Basic SQL Queries

Most basic SQL query functionality is supported:

```sql
toydb> SELECT * FROM studios;
1|Mosfilm
2|Lionsgate
3|StudioCanal
4|Warner Bros
5|Focus Features

toydb> SELECT title, rating FROM movies WHERE released >= 2000 ORDER BY rating DESC LIMIT 3;
Inception|8.8
Eternal Sunshine of the Spotless Mind|8.3
Gravity|7.7
```

Column headers can be enabled with `!headers on`:

```sql
toydb> !headers on
Headers enabled

toydb> SELECT id, name AS genre FROM genres;
id|genre
1|Science Fiction
2|Action
3|Drama
4|Comedy
```

## Expressions

All common mathematical operators are implemented:

```sql
toydb> SELECT 1 + 2 * 3;
7

toydb> SELECT (1 + 2) * 4 / -3;
-4

SELECT 3! + 7 % 4 - 2 ^ 3;
1
```

Complete 64-bit floating point support is also provided, included handling of infinity and NaN:

```sql
toydb> SELECT 3.14 * 2.718;
8.53452

toydb> SELECT 1.0 / 0.0;
inf

toydb> SELECT 1e10 ^ 8;
100000000000000000000000000000000000000000000000000000000000000000000000000000000

toydb> SELECT 1e10 ^ 8 / INFINITY, 1e10 ^ 1e10, INFINITY / INFINITY;
0|inf|NaN
```

And of course three-valued logic:

```sql
toydb> SELECT TRUE AND TRUE, TRUE AND FALSE, TRUE AND NULL, FALSE AND NULL;
TRUE|FALSE|NULL|FALSE

toydb> SELECT TRUE OR FALSE, FALSE OR FALSE, TRUE OR NULL, FALSE OR NULL;
TRUE|FALSE|TRUE|NULL

toydb> SELECT NOT TRUE, NOT FALSE, NOT NULL;
FALSE|TRUE|NULL
```

Which would be useless without comparison operators for all types:

```sql
toydb> SELECT 3 > 1, 3 <= 1, 3 = 3.0;
TRUE|FALSE|TRUE

toydb> SELECT 'a' = 'A', 'foo' > 'bar', '👍' != '👎';
FALSE|TRUE|TRUE

toydb> SELECT INFINITY > -INFINITY, NULL = NULL;
TRUE|NULL
```

## Joins

No SQL database would be complete without joins, and toyDB supports most join types such as
inner joins (both implicit and explicit):

```sql
toydb> SELECT m.id, m.title, g.name FROM movies m JOIN genres g ON m.genre_id = g.id LIMIT 4;
1|Stalker|Science Fiction
2|Sicario|Action
3|Primer|Science Fiction
4|Heat|Action

toydb> SELECT m.id, m.title, g.name FROM movies m, genres g WHERE m.genre_id = g.id LIMIT 4;
1|Stalker|Science Fiction
2|Sicario|Action
3|Primer|Science Fiction
4|Heat|Action
```

Left and right outer joins:

```sql
toydb> SELECT s.id, s.name, g.name FROM studios s LEFT JOIN genres g ON s.id = g.id;
1|Mosfilm|Science Fiction
2|Lionsgate|Action
3|StudioCanal|Drama
4|Warner Bros|Comedy
5|Focus Features|NULL

toydb> SELECT g.id, g.name, s.name FROM genres g RIGHT JOIN studios s ON g.id = s.id;
1|Science Fiction|Mosfilm
2|Action|Lionsgate
3|Drama|StudioCanal
4|Comedy|Warner Bros
NULL|NULL|Focus Features
```

As well as cross joins (both implicit and explicit):

```sql
toydb> SELECT g.name, s.name FROM genres g, studios s WHERE s.name < 'S';
Science Fiction|Mosfilm
Science Fiction|Lionsgate
Science Fiction|Focus Features
Action|Mosfilm
Action|Lionsgate
Action|Focus Features
Drama|Mosfilm
Drama|Lionsgate
Drama|Focus Features
Comedy|Mosfilm
Comedy|Lionsgate
Comedy|Focus Features
```

We can join on arbitrary predicates, such as joining movies with any genres whose name is
ordered after the movie's title:

```sql
toydb>  SELECT   m.title, g.name
        FROM     movies m JOIN genres g ON g.name > m.title
        ORDER BY m.title, g.name;

21 Grams|Action
21 Grams|Comedy
21 Grams|Drama
21 Grams|Science Fiction
Birdman|Comedy
Birdman|Drama
Birdman|Science Fiction
Eternal Sunshine of the Spotless Mind|Science Fiction
Gravity|Science Fiction
Heat|Science Fiction
Inception|Science Fiction
Lost in Translation|Science Fiction
Primer|Science Fiction
```

And we can join multiple tables, even using the same table multiple times - like in this example
where we find all science fiction movies released since 2000 by studios that have released any 
movie rated 8 or higher:

```sql
toydb> SELECT   m.id, m.title, g.name AS genre, m.released, s.name AS studio
       FROM     movies m JOIN genres g ON m.genre_id = g.id,
                studios s JOIN movies good ON good.studio_id = s.id AND good.rating >= 8
       WHERE    m.studio_id = s.id AND m.released >= 2000 AND g.id = 1
       ORDER BY m.title ASC;

7|Gravity|Science Fiction|2013|Warner Bros
10|Inception|Science Fiction|2010|Warner Bros
5|The Fountain|Science Fiction|2006|Warner Bros
```

## Explain

When optimizing complex queries with several joins, it can often be useful to inspect the query
plan via an `EXPLAIN` query, as with the previous join example:

```sql
toydb> EXPLAIN
       SELECT   m.id, m.title, g.name AS genre, m.released, s.name AS studio
       FROM     movies m JOIN genres g ON m.genre_id = g.id,
                studios s JOIN movies good ON good.studio_id = s.id AND good.rating >= 8
       WHERE    m.studio_id = s.id AND m.released >= 2000 AND g.id = 1
       ORDER BY m.title ASC;

Order: m.title asc
└─ Projection: m.id, m.title, g.name, m.released, s.name
   └─ HashJoin: inner on m.studio_id = s.id
      ├─ HashJoin: inner on m.genre_id = g.id
      │  ├─ Filter: m.released > 2000 OR m.released = 2000
      │  │  └─ IndexLookup: movies as m column genre_id (1)
      │  └─ KeyLookup: genres as g (1)
      └─ HashJoin: inner on s.id = good.studio_id
         ├─ Scan: studios as s
         └─ Scan: movies as good (good.rating > 8 OR good.rating = 8)
```

Here, we can see that the planner does a primary key lookup on `genres` and an index lookup on
`movies.genre_id`, filtering the resulting movies by release year and joining them. It also
does full table scans of `studios` and `movies` (to find the good movies) and joins them, pusing
the `rating >= 8` filter down to the `movies` table scan. The results of these two joins are also
joined to produce the final result, which is then formatted and sorted.

## Aggregates

Most basic aggregate functions are supported:

```sql
toydb> SELECT COUNT(*), MIN(rating), MAX(rating), AVG(rating), SUM(rating) FROM movies;
12|6.9|8.8|7.841666666666668|94.10000000000001
```

We can group by values and filter the aggregate results:

```sql
toydb> SELECT s.id, s.name, AVG(m.rating) AS average
       FROM movies m JOIN studios s ON m.studio_id = s.id
       GROUP BY s.id, s.name
       HAVING average > 7.8
       ORDER BY average DESC, s.name ASC;
1|Mosfilm|8.149999999999999
4|Warner Bros|7.919999999999999
5|Focus Features|7.900000000000001
```

And we can combine aggregate functions with arbitrary expressions, both inside and outside:

```sql
toydb> SELECT s.id, s.name, ((MAX(rating^2) - MIN(rating^2)) / AVG(rating^2)) ^ (0.5) AS spread
       FROM movies m JOIN studios s ON m.studio_id = s.id
       GROUP BY s.id, s.name
       HAVING MAX(rating) - MIN(rating) > 0.5
       ORDER BY spread DESC;
4|Warner Bros|0.6373540990222496
5|Focus Features|0.39194971607693424
```

## Transactions

toyDB supports ACID transactions via MVCC-based snapshot isolation. This provides atomic
transactions with good isolation without taking out locks or writes blocking reads. As a
basic example, the below transaction is rolled back without taking effect:

```sql
toydb> BEGIN;
Began transaction 131

toydb:131> INSERT INTO genres VALUES (5, 'Western');
toydb:131> SELECT * FROM genres;
1|Science Fiction
2|Action
3|Drama
4|Comedy
5|Western
toydb:131> ROLLBACK;
Rolled back transaction 131

toydb> SELECT * FROM genres;
1|Science Fiction
2|Action
3|Drama
4|Comedy
```

We'll demonstrate transactions by covering most common transaction anomalies given two
concurrent sessions, and show how toyDB prevents these anomalies in all cases but one. In these
examples, the left half is user A and the right is user B, and time flows downwards, such that 
commands on the same line happen at the same time.

**Dirty write:** an uncommitted write by A should not be affected by a concurrent B write.

```sql
a> BEGIN;
a> INSERT INTO genres VALUES (5, 'Western');
                                                 b> INSERT INTO genres VALUES (5, 'Romance');
                                                 Error: Serialization failure, retry transaction
a> SELECT * FROM genres WHERE id = 5;
5|Western
```

The serialization failure here occurs because the first write always wins. This may not be an
optimal strategy, but it is correct in terms of preventing serialization anomalies.

**Dirty read:** an uncommitted write by A should not be visible to B until committed.

```sql
a> BEGIN;
a> INSERT INTO genres VALUES (5, 'Western');
                                                 b> SELECT * FROM genres WHERE id = 5;
                                                 No rows returned
a> COMMIT;
                                                 b> SELECT * FROM genres WHERE id = 5;
                                                 5|Western
```

**Lost update:** when A and B both read a value, before updating it in turn, the first write should
not be overwritten by the second.

```sql
a> BEGIN;                                          b> BEGIN;
a> SELECT title, rating FROM movies WHERE id = 2;  b> SELECT title, rating FROM movies WHERE id = 2;
Sicario|7.6                                        Sicario|7.6
a> UPDATE movies SET rating = 7.8 WHERE id = 2;
                                                   b> UPDATE movies SET rating = 7.7 WHERE id = 2;
                                                   Error: Serialization failure, retry transaction
a> COMMIT;
```

**Fuzzy read:** B should not see a value suddenly change in its transaction, even if A commits a 
new value.

```sql
a> BEGIN;                                        b> BEGIN;
                                                 b> SELECT * FROM genres WHERE id = 1;
                                                 1|Science Fiction
a> UPDATE genres SET name = 'Scifi' WHERE id = 1;
a> COMMIT;
                                                 b> SELECT * FROM genres WHERE id = 1;
                                                 1|Science Fiction
                                                 b> COMMIT;

                                                 b> SELECT * FROM genres WHERE id = 1;
                                                 1|Scifi
```

**Read skew:** if A reads two values, and B modifies the second value in between the reads, A 
should see the old second value.

```sql
a> BEGIN;
a> SELECT * FROM genres WHERE id = 2;
2|Action
                                                 b> BEGIN;
                                                 b> UPDATE genres SET name = 'Drama' WHERE id = 2;
                                                 b> UPDATE genres SET name = 'Action' WHERE id = 3;
                                                 b> COMMIT;
a> SELECT * FROM genres WHERE id = 3;
3|Drama
```

**Phantom read:** when A runs a query with some predicate, and B commits a matching write, A should
not see the write when rerunning the query.

```sql
a> BEGIN;
a> SELECT * FROM genres WHERE id > 2;
3|Drama
4|Comedy
                                                 b> INSERT INTO genres VALUES (5, 'Western');
a> SELECT * FROM genres WHERE id > 2;
3|Drama
4|Comedy
```

**Write skew:**  when A reads row X and writes it to row Y, B should not concurrently be able to
read row Y and write it to row X.

```sql
a> BEGIN;                                        b> BEGIN;
a> SELECT * FROM genres WHERE id = 2;
2|Action
                                                 b> SELECT * FROM genres WHERE id = 3;
                                                 3|Drama
                                                 b> UPDATE genres SET name = 'Drama' WHERE id = 2;
a> UPDATE genres SET name = 'Action' WHERE id = 3;
a> COMMIT;                                       b> COMMIT;
```

Here, the writes actually go through. This anomaly is not protected against by snapshot isolation, 
and thus not by toyDB either. Doing so would require implementing serializable snapshot isolation. 
However, this is the only common serialization anomaly not handled by toyDB, and is not among the
most serious.

## Time-Travel Queries

Since toyDB uses MVCC for transactions and keeps all historical versions, the state of the database
can be queried at any arbitrary point in the past. toyDB uses incremental transaction IDs as
logical timestamps:

```sql
toydb> SELECT * FROM genres;
1|Science Fiction
2|Drama
3|Action
4|Comedy

toydb> BEGIN;
Began transaction 173
toydb:173> UPDATE genres SET name = 'Scifi' WHERE id = 1;
toydb:173> INSERT INTO genres VALUES (5, 'Western');
toydb:173> COMMIT;
Committed transaction 173

toydb> SELECT * FROM genres;
1|Scifi
2|Drama
3|Action
4|Comedy
5|Western

toydb> BEGIN READ ONLY AS OF SYSTEM TIME 172;
Began read-only transaction 175 in snapshot at version 172
toydb@172> SELECT * FROM genres;
1|Science Fiction
2|Drama
3|Action
4|Comedy
```