import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.parsing.json.JSON.parseFull

object Application {
  private def parseJson(jsonString: String) = {
    parseFull(jsonString).get.asInstanceOf[Map[String, Any]]
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SearchEngine")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("src/main/resources/test.json")
    val counts = textFile.flatMap(line => parseJson(line)("text").asInstanceOf[String].split(" "))
                          .map(word => (word, 1))
                          .reduceByKey(_ + _)
    counts.saveAsTextFile("output.txt")
  }
}
