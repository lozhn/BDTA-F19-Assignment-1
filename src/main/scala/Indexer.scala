import org.apache.spark.{SparkConf, SparkContext}

object Indexer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val outputPath = args(1)
  }
}
