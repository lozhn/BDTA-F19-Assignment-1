import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.reflect.io.Path
import scala.util.parsing.json.JSON.parseFull


object Indexer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val docPath = args(0)
    val outIndexPath = args(1)

    val doc = sc.textFile(docPath)
    // TODO: add batch file processing for indexers
    val compactIndex = CompactIndex.create_index_from_doc(doc)
    // To add next file do this:
    //    val new_doc = sc.textFile(next_file)
    //    val joined_index = compactIndex.join_index(CompactIndex.create_index_from_doc(new_doc))
  }

}
