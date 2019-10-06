import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Indexer {
  /***
   *
   * @param args:
   *            args[0]: filesForIndexing - path to directory with wiki dump files: Path
   *            args[1]: indexPath - path to save and load index files: Path
   *            args[2]: mode - building from scratch or adding new files to index: build | add
   */

  def main(args: Array[String]): Unit = {
    val spark = initSpark()
    var index: CompactIndex = null
    val filesForIndexing = args(0)
    val indexPath = args(1)
    val mode = args(2)

    index = CompactIndex.buildIndex(filesForIndexing, spark)
    if (mode == "add") {
      val indexPath = args(2)
      val loaded_index = CompactIndex.load(indexPath, spark)
      index.join_index(loaded_index)
    }
    index.save(indexPath) // 13 seconds to save EnWikiSmall index

    /*** Timing experiments
    time({
      index = CompactIndex.buildIndex("src/main/resources/EnWikiSmall", spark)
      println(index.words.count(), index.docs.count()) // about 108 seconds on EnWikiSmall
    })

    // Saving and loading is time consuming due to internal conversion to RDD[Record]
    time({
      index.save("src/main/resources/index.out") // 13 seconds to save EnWikiSmall index
    })
    time({
      index = CompactIndex.load("src/main/resources/index.out", spark) // 50 seconds to load index
      println(index.words.count(), index.docs.count())
    })
     ***/
  }

  private def initSpark(): SparkSession = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    SparkSession.builder().appName("SearchEngine").master("local").getOrCreate()
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000 + "s")
    result
  }
}
