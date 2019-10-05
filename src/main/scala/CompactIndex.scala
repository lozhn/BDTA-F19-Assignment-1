import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, HashSet}

case class CompactIndex(docs: RDD[(String, HashMap[String, Int])],
                        words: RDD[(String, HashSet[String])]) {

  val initialSet = HashSet.empty[String]
  val initialMap = HashMap.empty[String, Int]
  val addToSet = (s: HashSet[String], v: String) => s += v
  val addToMap = (s: HashMap[String, Int], v: Tuple2[String, Int]) => s += v
  val mergeSets = (p1: HashSet[String], p2: HashSet[String]) => p1 ++= p2
  val mergeMaps = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2

  def join_index(index: CompactIndex): CompactIndex = {
    val new_words_index = index.words.union(words)
      .aggregateByKey(initialSet)(mergeSets, mergeSets)
    val new_docs_index = index.docs.union(docs)
      .aggregateByKey(initialMap)(mergeMaps, mergeMaps)
    CompactIndex(new_docs_index, new_words_index)
  }

}
