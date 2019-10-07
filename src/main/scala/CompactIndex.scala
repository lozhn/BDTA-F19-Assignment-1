import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import implicits._

import scala.collection.mutable.{HashMap, HashSet}

final case class Record(title: String, word: String, freq: Int)

final case class DataRow(title: String, text: String)

final case class CompactIndex(docs: RDD[(String, HashMap[String, Int])], // doc_name: {word: freq}
                              words: RDD[(String, HashSet[String])]) // word: {doc_name}
{
  // TODO: check corresponding immutable collections
  def join_index(index: CompactIndex): CompactIndex = {
    val new_words_index = index.words.union(words)
      .aggregateByKey(CompactIndex.initialSet)(CompactIndex.mergeSets, CompactIndex.mergeSets)
    val new_docs_index = index.docs.union(docs)
      .aggregateByKey(CompactIndex.initialMap)(CompactIndex.mergeMaps, CompactIndex.mergeMaps)
    CompactIndex(new_docs_index, new_words_index)
  }

  // TODO: Find the way of serializing without using intermediate records
  def save(outPath: String): Unit = {
    this.docs.flatMap({ case (doc, words) =>
      words.map({ case (word, freq) => (doc, word, freq) })
    }).saveAsObjectFile(outPath)
  }
}

object CompactIndex {
  private val initialSet = HashSet.empty[String]
  private val initialMap = HashMap.empty[String, Int]
  private val addToSet = (s: HashSet[String], v: String) => s += v
  private val addToMap = (s: HashMap[String, Int], v: (String, Int)) => s += v
  private val mergeSets = (p1: HashSet[String], p2: HashSet[String]) => p1 ++= p2
  private val mergeMaps = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2

  // TODO: Find the way without using intermediate records
  def load(path: String, spark: SparkSession): CompactIndex = {
    val records = spark.sparkContext.objectFile[(String, String, Int)](path)
      .map({ case (title, word, freq) => Record(title, word, freq) })
    buildIndexOnRecords(records)
  }

  def buildIndex(path: String = null, spark: SparkSession): CompactIndex = {
    buildIndexOnRecords(buildRecords(path, spark))
  }

  private def buildIndexOnRecords(records: RDD[Record]): CompactIndex = {
    val docs_index = records.map({ case Record(title, word, freq) => (title, (word, freq)) })
      .aggregateByKey(initialMap)(addToMap, mergeMaps)

    val words_index = records.map({ case Record(title, word, _) => (word, title) })
      .aggregateByKey(initialSet)(addToSet, mergeSets)

    CompactIndex(docs_index, words_index)
  }

  private def buildRecords(path: String = null, spark: SparkSession): RDD[Record] = {
    import spark.implicits._

    if (path == null) {
      val path = "src/main/resources/EnWikiSmall"
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



