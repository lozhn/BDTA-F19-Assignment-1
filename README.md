# SearchEngine (Spark+Scala) project
In this homework, we are implementing a simple search engine with Spark. It supports indexing and document search. The goal is to practice programming with Spark, and search complexity is not a primary measure of performance. We implemented document indexing using RDD, the ranger used two methods: Basic Vector Space Model and BM25.
## args for submit

### Indexer
```bash
$ spark-submit --class Indexer app.jar <input>  <output> <mode> [<indexPath>]
 
# <input> - path, that need to index
# <output> - path, where to save index
# <mode>(build|add)  - build new one or add to existing
# <indexPath> - path to load existing index, if mode=add
```
#### example

```bash
$ spark-submit --class Indexer app.jar /EnWikiSmall index build
```
### Ranker

```bash
$ spark-submit --class Ranker app.jar <input> <method> <query>

#<input> - path for loading index
#<method> - naive(based on vector dot product) or bm25
#<query> - query to find relevant document
```
#### example

```bash
$ spark-submit --class Ranker app.jar index naive "hello world"
```

## Contribute by
### Arsiniy Poyezzhayev
### Nikita Lozhnikov
### Sergei Bakaleinik