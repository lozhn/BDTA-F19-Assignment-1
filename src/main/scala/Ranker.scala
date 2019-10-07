import implicits._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object Ranker {
  private val queryProcess = (s: String) => s.split("\\s").map(_.sanitizeTrimLower).filter(_.length > 1)

  def main(args: Array[String]): Unit = {
    // // TODO uncomment all next and test
    //    val conf = new SparkConf().setAppName("SearchEngine").setMaster("local")
    //    val sc = new SparkContext(conf)
    //    val query = "some query"
    //    val index = Indexer.load()
    //
    //    val k = 1.2
    //    val b = 0.75
    //    val D = index.docs.count()
    //    val avgdl = index.docs.map({case (_, map) => map.size}).reduce(_+_) / D
    //    val param = k * (1 - b + b * D / avgdl)
    //
    //    // preprocessed query
    //    val queryTerms = queryProcess(query)
    //    //  inner product
    //    val q_map = sc.parallelize(query.split(" "))
    //          .map(_.toLowerCase)
    //          .map(word => (word, 1)).reduceByKey(_ + _)
    //    val related_doc = index.docs.map(doc=>{
    //      val rank = doc.word_freq.map({case(k, v) => {
    //        val q_val = q_map.getOrElse(k, 0)
    //        if(q_val != 0)
    //          v*q_val
    //        else
    //          0
    //      }}).reduce(_+_)
    //      (doc.doc, rank)
    //    }).sortBy(_._2,ascending = false)
    //    related_doc.take(related_doc.count().toInt).map(println)
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