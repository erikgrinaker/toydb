# References

This is the main research material I used while building toyDB. It is a subset of my
[reading list](https://github.com/erikgrinaker/readings).

## Introduction

Andy Pavlo's CMU lectures are an absolutely fantastic introduction to database internals:

- 🎥 [CMU 15-445 Intro to Database Systems](https://www.youtube.com/playlist?list=PLSE8ODhjZXjbohkNBWQs_otTrBTrjyohi) (A Pavlo 2019)
- 🎥 [CMU 15-721 Advanced Database Systems](https://www.youtube.com/playlist?list=PLSE8ODhjZXjasmrEd2_Yi1deeE360zv5O) (A Pavlo 2020)

Martin Kleppman has written an excellent overview of database technologies and concepts, while Alex
Petrov goes in depth on implementation of storage engines and distributed systems algorithms:

- 📖 [Designing Data-Intensive Applications](https://dataintensive.net/) (M Kleppmann 2017)
- 📖 [Database Internals](https://www.databass.dev) (A Petrov 2019)

## Raft

The Raft consensus algorithm is described in a very readable paper by Diego Ongaro, and in a talk
given by his advisor John Ousterhout:

- 📄 [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) (D Ongaro, J Ousterhout 2014)
- 🎥 [Designing for Understandability: The Raft Consensus Algorithm](https://www.youtube.com/watch?v=vYp4LYbnnW8) (J Ousterhout 2016)

However, Raft has several subtle pitfalls, and Jon Gjengset's student guide was very helpful in
drawing attention to these:

- 🔗 [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/) (J Gjengset 2016)

## Parsing

Thorsten Ball has written a very enjoyable hands-on introduction to parsers where he implements
first an interpreter and then a compiler for the made-up Monkey programming language (in Go):

- 📖 [Writing An Interpreter In Go](https://interpreterbook.com) (T Ball 2016) 
- 📖 [Writing A Compiler In Go](https://compilerbook.com) (T Ball 2018)

The toyDB expression parser is inspired by a blog post by Eli Bendersky describing the precedence
climbing algorithm, which is the algorithm I found the most elegant:

- 💬 [Parsing Expressions by Precedence Climbing](https://eli.thegreenplace.net/2012/08/02/parsing-expressions-by-precedence-climbing) (E Bendersky 2012)

## Transactions

Jepsen (i.e. Kyle Kingsbury) has an excellent overview of consistency and isolation models, which 
is very helpful in making sense of the jungle of overlapping and ill-defined terms:

- 🔗 [Consistency Models](https://jepsen.io/consistency) (Jepsen 2016)

For more background on this, in particular on how snapshot isolation provided by the MVCC
transaction engine used in toyDB does not fit into the traditional SQL isolation levels, the
following classic papers were useful:

- 📄 [A Critique of ANSI SQL Isolation Levels](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf) (H Berenson et al 1995)
- 📄 [Generalized Isolation Level Definitions](http://pmg.csail.mit.edu/papers/icde00.pdf) (A Adya, B Liskov, P ONeil 2000)

As for actually implementing MVCC, I found blog posts to be the most helpful:

- 💬 [Implementing Your Own Transactions with MVCC](https://levelup.gitconnected.com/implementing-your-own-transactions-with-mvcc-bba11cab8e70) (E Chance 2015)
- 💬 [How Postgres Makes Transactions Atomic](https://brandur.org/postgres-atomicity) (B Leach 2017)
