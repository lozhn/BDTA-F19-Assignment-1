import implicits._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


object Ranker {
  /** *
   *
   * @param args :
   *             args[0]: query: String
   *             args[1]: LoadPath - path to load index files: Path
   *             args[2]: SavePath - path to save: Path
   */
  def main(args: Array[String]): Unit = {
    val spark = initSpark()
    val query = "developers defines" // args[0]
    val loadPath = "./index" // args[1]
    val outputPath = "./output" // args[2]
    Indexer.time {
      val index = CompactIndex.load(path = loadPath, spark = spark)
      val sc = spark.sparkContext
      val docs_index = index.docs
      val D = spark.sparkContext.broadcast[Long](docs_index.count())
      // preprocessed query
      val queryTerms = queryProcess(query)
      var _idfs: RDD[(String, Double)] = docs_index
        .filter({ case (w, _) => queryTerms.contains(w) })
        .map({ case (w, docs) =>
          val n = docs.size
          val idf = math.log((D.value - n + 0.5) / (n + 0.5)) + 1e-10 // noise to avoid zero-division etc.
          (w, idf)
        })
      Indexer.time {
        val idfs: collection.Map[String, Double] = _idfs.collectAsMap()
        val q_map = mutable.HashMap(sc.parallelize(queryTerms)
          .map(word => (word, 1))
          .reduceByKey(_ + _)
          .collect(): _*)
        val related_doc: RDD[(String, Double)] = docs_index.map({ case (doc, tf) =>
          val rank = tf.map({ case (k, v) =>
            val idfs_v: Double = idfs.getOrElse(k, 0)
            if (idfs_v != 0.0)
              q_map.getOrElse(k, 0) * v / (idfs_v * idfs_v)
            else
              0
          }).sum
          (doc, rank)
        }).sortBy(_._2, ascending = false)//.filter({ case (k, v) => v > 0 })
        related_doc.take(10).map(println)
        //related_doc.saveAsTextFile(outputPath+"/vector")
      }
      // TODO: broadcast to sparkContext
      val k = spark.sparkContext.broadcast[Double](2.0)
      val b = spark.sparkContext.broadcast[Double](0.75)
      val avgdl = spark.sparkContext.broadcast[Double](docs_index.map({ case (_, map) => map.size }).reduce(_ + _) / docs_index.count())
      val param = spark.sparkContext.broadcast[Double](k.value * (1 - b.value + b.value * D.value / avgdl.value))

      Indexer.time {
        val idfs = spark.sparkContext.broadcast(_idfs.collect())
        // BM25
        // iterate the whole index, calc score for each
        val rankedDocs = docs_index.map({ case (doc, tf) =>
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

  private val queryProcess = (s: String) => s.split("\\s").map(_.sanitizeTrimLower).filter(_.length > 1)

  def initSpark(): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    SparkSession.builder().appName("SearchEngine").master("local").getOrCreate()
  }
}