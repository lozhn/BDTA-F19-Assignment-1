import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON.parseFull


object Indexer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)
  }

  def standardize(word: String): String = {
    word.replaceAll("""('s)|([\p{Punct}&&[^-]])""", " ")
      .trim
      .toLowerCase
  }

  def parseJson(jsonString: String): Map[String, String] = {
    parseFull(jsonString).get.asInstanceOf[Map[String, String]]
  }
}
