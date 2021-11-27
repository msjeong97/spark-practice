// scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.implicits._

object SparkBooleanContion extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkDFApi")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val df = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load("/Users/minseop/minseop/spark/data/retail-data/by-day/2010-12-01.csv")





    spark.stop()
  }
}
// scalastyle:on println
