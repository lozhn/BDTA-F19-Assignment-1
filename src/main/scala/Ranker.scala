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


  // --class Ranker app.jar "/path/to/index" bm25 "query query"
  def main(args: Array[String]): Unit = {
    var ranker: String = ""
    var query: String = ""
    var path: String = ""

    args match {
      case Array(_path: String, _ranker: String, _query: String) =>
        path = _path
        ranker = _ranker
        query = _query
      case _ =>
        println("""Usage: <index path [/egypt/indexMedium]> <ranker: naive|bm25> "<query>" """)
        return
    }

    println(s"Running $ranker based on index from $path with query - $query")

    val spark = initSpark()
    val index = CompactIndex.load(path = path, spark = spark)
    val D = spark.sparkContext.broadcast[Long](index.docs.count())

    // preprocessed query
    val queryTerms = query.tokenize
    val idfs: RDD[(String, Double)] = index.words
      .filter { case (w, _) => queryTerms.contains(w) }
      .map {
        case (w, docs) =>
          val n = docs.size
          val idf = math.log((D.value - n + 0.5) / (n + 0.5)) + 1e-10 // noise to avoid zero-division etc.
          (w, idf)
      }

    Indexer.time {
      ranker match {
        case "bm25" =>
          println("bm25>")
          bm25(spark, index, query, idfs)
        case "naive" =>
          println("naive>")
          naive(spark, index, query, idfs)
        case _ =>
          println("""Ranker can be either bm25 or naive""")
      }
    }
  }

  def naive(spark: SparkSession, index: CompactIndex, query: String, idfs: RDD[(String, Double)]): Unit = {
    val sc = spark.sparkContext
    var _idfs = sc.broadcast(idfs.collectAsMap())

    val q_map = mutable.HashMap(sc.parallelize(query.tokenize)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect(): _*)

    val rankedDocs: RDD[(Double, String)] = index.docs.map({ case (doc, tf) =>
      val rank = tf.map {
        case (k, v) =>
          val idfs_v = _idfs.value.getOrElse(k, 0.0)
          if (idfs_v != 0.0) {
            q_map.getOrElse(k, 0) * v / (idfs_v * idfs_v)
          } else {
            0
          }
      }.sum

      (rank, doc)
    }).sortByKey(ascending = false)

    rankedDocs.take(10).foreach(println)
  }

  def bm25(spark: SparkSession, index: CompactIndex, query: String, idfs: RDD[(String, Double)]): Unit = {
    val k = spark.sparkContext.broadcast[Double](2.0)
    val b = spark.sparkContext.broadcast[Double](0.75)
    val D = spark.sparkContext.broadcast[Long](index.docs.count())
    val avgdl = spark.sparkContext.broadcast[Double](index.docs.map { case (_, map) => map.size }.mean())
    val param = spark.sparkContext.broadcast[Double](k.value * (1 - b.value + b.value * D.value / avgdl.value))

    val _idfs = spark.sparkContext.broadcast(idfs.collect())
    // BM25
    // iterate the whole index, calc score for each
    val rankedDocs = index.docs.map { case (doc, tf) =>
      val rank = _idfs.value.map { case (w, idf) =>
        val t = tf.getOrElse(w, 0)
        idf * (t * (k.value + 1)) / (t + param.value)
      }.sum

      (rank, doc)
    }.sortByKey(ascending = false)

    rankedDocs.take(10).foreach(println)
  }

  def initSpark(): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    SparkSession.builder().appName("SearchEngine").master("local").getOrCreate()
  }
}