import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashSet
import scala.util.parsing.json.JSON.parseFull


object Indexer {

  private val initialSet = HashSet.empty[Any]
  private val addToSet = (s: HashSet[Any], v: Any) => s += v
  private val mergeSets = (p1: HashSet[Any], p2: HashSet[Any]) => p1 ++= p2
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
      .aggregateByKey(initialSet)(addToSet, mergeSets)
    val words_index = twf.map({ case ((title, word), freq) => (word, title) })
      .aggregateByKey(initialSet)(addToSet, mergeSets)

    docs_index.saveAsTextFile(outIndexPath + "/docs.index")
    words_index.saveAsTextFile(outIndexPath + "/words.index")
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
