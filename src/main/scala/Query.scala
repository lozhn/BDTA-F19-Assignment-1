import implicits._


object Query {
  private val queryProcess = (s: String) => s.split("\\s").map(_.sanitizeTrimLower).filter(_.length > 1)

  def main(args: Array[String]): Unit = {
//    val query = "some query"
//    val index = Indexer.load()
//
//    val k = 1.2
//    val b = 0.75
//    val D = index.docs.count()
//    val avgdl = index.docs.map({case (_, map) => map.size}).reduce(_+_) / index.docs.count()
//    val param = k * (1 - b + b * D / avgdl)
//
//    // preprocessed query
//    val queryTerms = queryProcess(query)
//
//    // idfs of words that are both in voc and query
//    val idf = index.words
//      .filter({case (w, _) => queryTerms.contains(w)})
//      .map({case (w, docs) =>
//        val n = docs.size
//        val idf = math.log((D - n + 0.5) / (n + 0.5)) + 1e-10 // noise to avoid zero-division etc.
//        (w, idf)
//      })
//
//    // BM25
//    // iterate the whole index, calc score for each
//    val rankedDocs = index.docs.map({ case (doc, tf) =>
//      val rank = idf.map({case (w, idf) =>
//        idf * ( tf(w) * (k + 1) ) / ( tf(w) + param )
//      }).sum()
//
//      (rank, doc)
//    }).sortByKey(ascending = false)
//
//    println(rankedDocs.take(10))
  }
}