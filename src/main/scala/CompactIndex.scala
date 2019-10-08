import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import implicits._

import scala.collection.mutable.{HashMap, HashSet}

/**
 * Just a tuple of information required for pre-processing the data
 *
 * @param title - doc title
 * @param word  - term
 * @param freq  - frequence of the term
 */
final case class Record(title: String, word: String, freq: Int)

/**
 * For JSON processing
 *
 * @param title - doc title
 * @param text  - corresponding text
 */
final case class DataRow(title: String, text: String)

/**
 * Case class for storing and I/O of index
 *
 * @param docs
 * @param words
 */
final case class CompactIndex(docs: RDD[(String, HashMap[String, Int])], // doc_name: {word: freq}
                              words: RDD[(String, HashSet[String])]) // word: {doc_name}
{
  // TODO: check corresponding immutable collections
  /**
   * Merge another index with this index
   *
   * @param index to merge with this
   * @return merged CompactIndex
   */
  def join_index(index: CompactIndex): CompactIndex = {
    val new_words_index = index.words.union(words)
      .aggregateByKey(CompactIndex.initialSet)(CompactIndex.mergeSets, CompactIndex.mergeSets)
    val new_docs_index = index.docs.union(docs)
      .aggregateByKey(CompactIndex.initialMap)(CompactIndex.mergeMaps, CompactIndex.mergeMaps)
    CompactIndex(new_docs_index, new_words_index)
  }

  /**
   * Saves the index to directory
   *
   * @param outPath - directory where to save index
   */
  def save(outPath: String): Unit = {
    this.docs.map({ case (doc, words) => (doc, words.toSeq) }).saveAsObjectFile(outPath + "_docs")
    this.words.map({ case (word, docs) => (word, docs.toSeq) }).saveAsObjectFile(outPath + "_words")
  }
}

/**
 * Companion class for CompactIndex initialization and static methods
 */
object CompactIndex {
  // Methods for aggregation of internal collections by aggregateByKey
  // For mutable HashMap and HashSet
  private val initialSet = HashSet.empty[String]
  private val initialMap = HashMap.empty[String, Int]
  private val addToSet = (s: HashSet[String], v: String) => s += v
  private val addToMap = (s: HashMap[String, Int], v: (String, Int)) => s += v
  private val mergeSets = (p1: HashSet[String], p2: HashSet[String]) => p1 ++= p2
  private val mergeMaps = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2

  /**
   * Load the CompactIndex from dir.
   * Internally it just unpacks Seq to more suitable collections
   *
   * @param path  : where the index stored
   * @param spark : current spark session
   * @return loaded CompactIndex
   */
  def load(path: String, spark: SparkSession): CompactIndex = {
    val docs = spark.sparkContext.objectFile[(String, Seq[(String, Int)])](path + "_docs")
      .map({ case (doc, words) => (doc, HashMap(words: _*)) })
    val words = spark.sparkContext.objectFile[(String, Seq[String])](path + "_words")
      .map({ case (word, docs) => (word, HashSet(docs: _*)) })
    CompactIndex(docs, words)
  }

  /**
   * Builds index directly from the JSONs
   *
   * @param path  to directory with JSONs or certain JSON
   * @param spark - current spark session
   * @return
   */
  def buildIndex(path: String = null, spark: SparkSession): CompactIndex = {
    buildIndexOnRecords(buildRecords(path, spark))
  }

  /**
   * Aggregates necessary information from RDD of 3-Tuples (title, word, freq) and builds CompactIndex
   *
   * @param records
   * @return CompactIndex
   */
  private def buildIndexOnRecords(records: RDD[Record]): CompactIndex = {
    val docs_index = records.map({ case Record(title, word, freq) => (title, (word, freq)) })
      .aggregateByKey(initialMap)(addToMap, mergeMaps)

    val words_index = records.map({ case Record(title, word, _) => (word, title) })
      .aggregateByKey(initialSet)(addToSet, mergeSets)

    CompactIndex(docs_index, words_index)
  }

  /**
   * Builds the RDD of 3-Tuples[Record] from initial JSONs
   *
   * @param path  to directory with JSONs or certain JSON
   * @param spark - current spark session
   * @return
   */
  private def buildRecords(path: String = null, spark: SparkSession): RDD[Record] = {
    import spark.implicits._

    if (path == null) {
      println("Path is null. Using EnWikiSmall")
      val path = "./resources/EnWikiSmall"
    }
    val ds = spark.read.json(path).as[DataRow]

    ds.flatMap(row =>
      row.text
        .split("\\s")
        .map(_.sanitizeTrimLower)
        .filter(_.length > 1)
        .map(w => ((row.title, w), 1))
    ).rdd
      .reduceByKey(_ + _)
      .map { case ((title, word), freq) => Record(title, word, freq) }
  }
}



