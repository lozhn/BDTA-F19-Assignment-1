# SearchEngine (Spark+Scala) project
In this homework, we are implementing a simple search engine with Spark. It supports indexing and document search. The goal is to practice programming with Spark, and search complexity is not a primary measure of performance. We implemented document indexing using RDD, the ranger used two methods: Basic Vector Space Model and BM25.
## args for submit

### Indexer
1. filesForIndexing - path, that need to index
2. indexSavePath - path for save index
3. mode - build or add - build new one or add to existing
4. indexLoadPath - path to load existing index, if mode=add

### Ranker
1. LoadPath - path for loading index
2. method - naive(based on vector dot product) or bm25
3. query - query for search through documents

## Contribute by
### Arsiniy Poyezzhayev
### Nikita Lozhnikov
### Sergei Bakaleinik