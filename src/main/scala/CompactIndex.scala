import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, HashSet}
import scala.util.parsing.json.JSON.parseFull


case class CompactIndex(docs: RDD[(String, HashMap[String, Int])], // doc_name: {word: freq}
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
}

object CompactIndex {
  private val initialSet = HashSet.empty[String]

  private val initialMap = HashMap.empty[String, Int]

  private val addToSet = (s: HashSet[String], v: String) => s += v

  private val addToMap = (s: HashMap[String, Int], v: Tuple2[String, Int]) => s += v

  private val mergeSets = (p1: HashSet[String], p2: HashSet[String]) => p1 ++= p2

  private val mergeMaps = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2

  def create_index_from_doc(doc: RDD[String]): CompactIndex = {
    val title_word_freq = parse_doc(doc)
    val docs_index = title_word_freq.map({ case Record(title, word, freq) => (title, (word, freq)) })
      .aggregateByKey(initialMap)(addToMap, mergeMaps)
    val words_index = title_word_freq.map({ case Record(title, word, _) => (word, title) })
      .aggregateByKey(initialSet)(addToSet, mergeSets)
    CompactIndex(docs_index, words_index)
  }

  private def standardize(word: String): String = {
    word.replaceAll("""('s)|([\p{Punct}&&[^-]])""", " ")
      .trim
      .toLowerCase
  }

  private def parseJson(jsonString: String): Map[String, String] = {
    parseFull(jsonString).get.asInstanceOf[Map[String, String]]
  }

  private def parse_doc(doc: RDD[String]): RDD[Record] = {
    val title_text = doc.map(line => {
      val json = parseJson(line)
      (json("title"), json("text"))
    })
    title_text.flatMap({ case (doc, text) =>
      val words = text.split("\\s")
      words.map(standardize)
        .filter(_.length > 1)
        .map(word => ((doc, word), 1))
    })
      .reduceByKey(_ + _)
      .map({ case ((title, word), freq) => Record(title, word, freq) })
  }
}


