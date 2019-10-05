import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.reflect.io.Path
import scala.util.parsing.json.JSON.parseFull

case class CompactIndex(
                         docs: RDD[Docs],
                         words: RDD[(String, HashSet[String])]
                       )

case class Docs(
                 doc: String,
                 word_freq: HashMap[String, Int]
               )

object Indexer {

  val initialSet = HashSet.empty[String]
  val initialMap = HashMap.empty[String, Int]
  val addToSet = (s: HashSet[String], v: String) => s += v
  val addToMap = (s: HashMap[String, Int], v: Tuple2[String, Int]) => s += v
  val mergeSets = (p1: HashSet[String], p2: HashSet[String]) => p1 ++= p2
  val mergeMaps = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2

  var sc: SparkContext;

  //  val words_index: RDD[(String, HashSet[String])]; // word: {doc}
  //  val docs_index: RDD[(String, HashSet[(String, Int)])]; // doc: {(word: freq)}

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val docPath = args(0)
    val outIndexPath = args(1)

    val doc = sc.textFile(docPath)
    val twf = parse_doc(doc)
    val docs_index = twf.map({ case ((title, word), freq) => (title, (word, freq)) })
      .aggregateByKey(initialMap)(addToMap, mergeMaps)
      .map({ case (title, maps) => Docs(title, maps) })

    val words_index = twf.map({ case ((title, word), freq) => (word, title) })
      .aggregateByKey(initialSet)(addToSet, mergeSets)

    val index = CompactIndex(docs_index, words_index)

    serialize(index)
  }

  def standardize(word: String): String = {
    word.replaceAll("""('s)|([\p{Punct}&&[^-]])""", " ")
      .trim
      .toLowerCase
  }

  def parseJson(jsonString: String): Map[String, String] = {
    parseFull(jsonString).get.asInstanceOf[Map[String, String]]
  }

  def parse_doc(doc: RDD[String]): RDD[((String, String), Int)] = {
    val title_text = doc.map(line => {
      val json = parseJson(line)
      (json("title"), json("text"))
    })

    title_text.flatMap({ case (doc, text) =>
      val words = text.split("\\s")
      words.map(standardize)
        .filter(_.length > 1)
        .map(word => ((doc, word), 1))
    }).reduceByKey(_ + _)
  }

  def add_docs(doc_files: RDD[String]) = {
    doc_files.map(file => sc.textFile(file)).map() {
      val index = load_index()
      val doc = sc.textFile(file)
      val d_title_word_freq = parse_doc(file)
      add_doc_to_index(doc, docs)

    }
  }


  //  def add_doc_to_index(doc: RDD[String],
  //                       docs_index: RDD[(String, HashSet[Any])],
  //                       words_index: RDD[(String, HashSet[Any])]):
  //  Tuple2[RDD[(String, HashSet[Any])], RDD[(String, HashSet[Any])]] = {
  //    val title_word_freq = parse_doc(doc)
  //    val docs = title_word_freq.map({ case ((title, word), freq) => (title, (word, freq)) }).aggregateByKey(initialSet)(addToSet, mergeSets)
  //    val words = title_word_freq.map({ case ((title, word), freq) => (word, title) }).aggregateByKey(initialSet)(addToSet, mergeSets)
  //    val new_words_index = words.union(words_index).aggregateByKey(initialSet)(mergeSets, mergeSets)
  //    val new_docs_index = docs.union(docs_index).aggregateByKey(initialSet)(mergeSets, mergeSets)
  //    (new_docs_index, new_words_index)
  //  }
}
