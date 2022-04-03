import org.apache.spark.sql.SparkSession

object VariousTests {

  var t1 = System.nanoTime()
  var t1InitTotal = t1

  // Creates a SparkSession
  val spark = SparkSession.builder.appName("FreqSeqPatterns").getOrCreate()
  var t2 = (System.nanoTime() - t1 ) / 1e9d
  println("Set up Spark time= " + t2 + " secs")
  // This import is needed to use the $-notation

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()

    spark.stop()

  }
}
