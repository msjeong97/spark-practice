// scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object SparkScalaExample extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkScalaExample")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val flightData = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/Users/minseop/minseop/spark/data/flight-data/csv/2015-summary.csv")
    flightData.show()
    
    val top3 = flightData.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "count")
      .sort(desc("count"))
      .take(3)

    println(s"top3 country")
    top3.foreach(println)

    spark.stop()
  }
}
// scalastyle:on println
