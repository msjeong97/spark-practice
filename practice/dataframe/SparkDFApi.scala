// scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDFApi extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkDFApi")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val flightData = spark.read
      .format("json")
      .load("/Users/minseop/minseop/spark/data/flight-data/json/2015-summary.json")

    val df_new_column_added = flightData.withColumn("count_as_string", col("count").cast("string"))
    // add new column which is casted to string from "count"
    println(added_new_column.schema)

    val df_column_renamed = flightData.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
    // dest ORIGIN_COUNTRY_NAME count
    println(df_column_renamed.columns.mkString(" "))

    val filtered_1_1 = flightData.filter(col("ORIGIN_COUNTRY_NAME") === "United States")
    val filtered_1_2 = flightData.where("ORIGIN_COUNTRY_NAME == \"United States\"")
    // true
    println(filtered_1_1.count == filtered_1_2.count)

    val filtered_2_1 = flightData.filter(col("count") > 100).orderBy(desc("count"))
    val filtered_2_2 = flightData.where("count > 100").orderBy(desc("count"))
    // same result
    println(filtered_2_1.show(10))
    println(filtered_2_2.show(10))



    spark.stop()
  }
}
// scalastyle:on println
