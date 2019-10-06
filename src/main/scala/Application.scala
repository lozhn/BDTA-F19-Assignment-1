import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import implicits._

final case class Row(title: String, text: String)

object Application {
  def initSpark(): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    SparkSession.builder().appName("SearchEngine").master("local").getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = initSpark()
    import spark.implicits._

    val path = "src/main/resources/EnWikiSmall"
    val ds = spark.read.json(path).as[DataRow]

    ds.flatMap(row =>
      row.text
        .split("\\s")
        .map(_.sanitizeTrimLower)
        .filter(_.length > 1)
        .map(w => ((w, row.title), 1))
    )

  }
}
