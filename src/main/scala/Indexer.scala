import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, HashSet}
import scala.util.parsing.json.JSON.parseFull


object Indexer {
  var sc: SparkContext

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    sc = new SparkContext(conf)

    val docPath = args(0)
    val outIndexPath = args(1)

    val doc = sc.textFile(docPath)
    // TODO: add batch file processing for indexers
    val empty_index = CompactIndex(sc.emptyRDD[(String, HashMap[String, Int])], sc.emptyRDD[(String, HashSet[String])])
    val compactIndex = CompactIndex.create_index_from_doc(doc)
    // To add next file do this:
    //    val new_doc = sc.textFile(next_file)
    //    val joined_index = compactIndex.join_index(CompactIndex.create_index_from_doc(new_doc))
  }


  // mock
//  def load(): CompactIndex = new CompactIndex(
//    RDD[(String, HashMap[String, Int])],
//    RDD[(String, HashSet[String])]
//  )

//  def batch_file_processor(files: List[String], batch_size: Int = 8): RDD[String] = {
//    files.iterator.grouped(batch_size).foreach(sc.textFile)
//  }


}
