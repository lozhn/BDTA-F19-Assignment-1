import implicits._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashSet


object Query {
  private val queryProcess = (s: String) => s.split("\\s").map(_.sanitizeTrimLower).filter(_.length > 1)

  def initSpark(): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    SparkSession.builder().appName("SearchEngine").master("local").getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = initSpark()
    import spark.implicits._
    val query = "carry easier"
    //    val index = CompactIndex.buildIndex(path = "src/main/resources/EnWikiSmall", spark = spark)
    //    index.save("./index")
    val index = CompactIndex.load(path = "./index", spark = spark)

    // TODO: broadcast to sparkContext
    val k = spark.sparkContext.broadcast[Double](2.0)
    val b = spark.sparkContext.broadcast[Double](0.75)
    val D = spark.sparkContext.broadcast[Long](index.docs.count())
    val avgdl = spark.sparkContext.broadcast[Double](index.docs.map({ case (_, map) => map.size }).reduce(_ + _) / index.docs.count())
    val param = spark.sparkContext.broadcast[Double](k.value * (1 - b.value + b.value * D.value / avgdl.value))


    // preprocessed query
    val queryTerms = queryProcess(query)

    Indexer.time {

      // var _idfs = queryTerms.map(word => (word, index.words.lookup(word).length))
      // idfs of words that are both in voc and query
      var _idfs: RDD[(String, Double)] = index.words
        .filter({ case (w, _) => queryTerms.contains(w) })
        .map({ case (w, docs) =>
          val n = docs.size
          val idf = math.log((D.value - n + 0.5) / (n + 0.5)) + 1e-10 // noise to avoid zero-division etc.
          (w, idf)
        })

      val idfs = spark.sparkContext.broadcast(_idfs.collect())

      // BM25
      // iterate the whole index, calc score for each
      val rankedDocs = index.docs.map({ case (doc, tf) =>
        val rank = idfs.value.map({ case (w, idf) =>
          val t = tf.getOrElse(w, 0)
          idf * (t * (k.value + 1)) / (t + param.value)
        }).sum

        (rank, doc)
      }).sortByKey(ascending = false)

      rankedDocs.take(10).map(println)
    }
  }
}