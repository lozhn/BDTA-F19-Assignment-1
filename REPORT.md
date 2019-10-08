# Assignment 1

## Team: 
- Arseniy Poyezzshayev (a.poezzhaev@innopolis.ru)
- Nikita Lozhnkov (n.lozhnikov@innopolis.ru)
- Sergey Bakaleynik (s.bakaleynik@innopolis.ru)

<sup>In alphabetical order</sup>

## Intro

In this homework, we are implementing a simple search engine with Spark. It supports indexing and document search. The goal is to practice programming with Spark, and search complexity is not a primary measure of performance. We implemented document indexing using RDD, the ranger used two methods: Basic Vector Space Model and BM25.

## Components 

`src/main/scala`

- **Indexer** (`Indexer.scala`)
    - **CompactIndex** (`CompactIndex.scala`)
- **Ranker** (`Ranker.scala`)
- **Misc**
    - **implicits** (`implicits.scala`)

### Indexer

```bash
$ spark-submit --class Indexer app.jar <input-path> <index-path> <cmd> [<old-index>]

# <input-path> - a path to the documents to be indexed
# <index-path> - a path where the index will be stored
# <cmd> - build|add  
#           - if build => <old-index> is not needed. 
#               it will build an index on the files located ad <input-path>
#           - if add => it will load index from <old-path>, 
#               add documents from <input-path> and save the updated index to <index-path>
```

Example 

```bash
$ spark-submit --class Indexer app.jar hdfs:///EnWikiMedium hdfs:///egypt/indexMedium build
```

**Indexer architecture**

We decided to use RDDs as a main data structure for our computations because it is quite low-level and without any SQL-like optimizations etc. This allowed us to feel the pain to investigate the issues connected with data flows. The initially proposed RDD of 3-Tuples `(doc, word, frequency)` is too redundant. We created the `CompactIndex` class which contained two internal indexes. One is a map of `Words: {word: Set(docs)}` another is nested map of `Docs: {doc: {word: TF}}`. These internal indices allowed us to effectively calculate TF, IDF, avgdl, |D| which are enough for both rankers. 

The main architectural decision was to support the adding of new documents to index on-fly. Therefore, we decide not to add the precomputed IDFs to words index, because we must recompute IDFs with every added document. Also, we assumed that the addition of duplicated documents is possible, therefore we keep the Sets of documents in which the certain word is occurred (not only number of documents). We created the method for appending the newly added documents to existing index.


### Ranker

Is an object class with `main` method.

```bash
$ spark-submit --class Ranker app.jar <input> <method> <query>

#<input> - path for loading index
#<method> - naive(based on vector dot product) or bm25
#<query> - query to find relevant document
```

Example

```bash
$ spark-submit --class Ranker app.jar hdfs:///egypt/indexMedium naive "hello world"
```

#### Naive

#### BM25 

## Problems

Precomputation of IDFs as well as TF/IDF for every term in every document may help both rankers. This computation may be performed on the immutable index and we added the method for it in CompactIndex class. We don’t serialize the precomputed TF/IDFs in files and must process it every time the index is loaded into memory. This means that our Rankers could exploit improved performance only if the Indexer program is loaded in memory (daemon process). However, according to the task we cannot use daemons to not occupy resources of the cluster. Every query ranking starts from loading dumped index and then ranking task. This is a drawback of our solution.

We faced the problem of RDDs serialization in objectFile, because there is no native support for every Scala collection in RDD serialization. Therefore, we created custom save and load methods in CompactIIndex, which internally saves and loads two separate RDDs (Words and Docs).

## Performance

Indexing operation is quite performant: 

* It **indexes** the whole dataset (EnWikiMedium) in about **6 minutes** on cluster. 
* **Loading** operation takes about **6 seconds**.
* **Appending** new docs takes about **1 minute** (not including building time) for new docs dataset of size comparable to EnWikiMedium.

Ranking is not that performant as we would like it to be:

* Naive ranker is usually 1.5 times faster than BM25, because our implementation of BM25 needs to do more O(n) operations like `.count()` and filtering was not the most efficient due to fullscan of the docs. Next time we won't use RDDs and fullscan
* BM25 worked better in terms of MAP 