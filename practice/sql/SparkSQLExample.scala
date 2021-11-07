// scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object SparkSQLExample extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkSQLExample")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val flightData = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/Users/minseop/minseop/spark/data/flight-data/csv/2015-summary.csv")
    flightData.show()

    flightData.createOrReplaceTempView("flight_data")

    val top3 = spark.sql("""
      SELECT DEST_COUNTRY_NAME, SUM(count) AS cnt
      FROM flight_data
      GROUP BY DEST_COUNTRY_NAME
      ORDER BY cnt DESC;
    """).take(3)

    println(s"top3 country")
    top3.foreach(println)

    spark.stop()
  }
}
// scalastyle:on println
