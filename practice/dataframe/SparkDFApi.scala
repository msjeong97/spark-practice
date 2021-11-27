// scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.implicits._

object SparkDFApi extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkDFApi")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val df = spark.read
      .format("json")
      .load("/Users/minseop/minseop/spark/data/flight-data/json/2015-summary.json")

    val df_new_column_added = df.withColumn("count_as_string", col("count").cast("string"))
    // add new column which is casted to string from "count"
    println(added_new_column.schema)

    val df_column_renamed = df.withColumnRenamed("DEST_COUNTRY_NAME", "dest")
    // dest ORIGIN_COUNTRY_NAME count
    println(df_column_renamed.columns.mkString(" "))

    val filtered_1_1 = df.filter(col("ORIGIN_COUNTRY_NAME") === "United States")
    val filtered_1_2 = df.where("ORIGIN_COUNTRY_NAME == \"United States\"")
    // true
    println(filtered_1_1.count == filtered_1_2.count)

    val filtered_2_1 = df.filter(col("count") > 100).orderBy(desc("count"))
    val filtered_2_2 = df.where("count > 100").orderBy(desc("count"))
    // same result
    println(filtered_2_1.show(10))
    println(filtered_2_2.show(10))

    val df_distinct_dest_country = df.select("DEST_COUNTRY_NAME").distinct()
    println(df_distinct_dest_country.count())

    val df1 = spark.read
      .format("json")
      .load("/Users/minseop/minseop/spark/data/flight-data/json/2015-summary.json")
    val df2 = spark.read
      .format("parquet")
      .load("/Users/minseop/minseop/spark/data/flight-data/parquet/2010-summary.parquet")
    val df_filtered = df1.union(df2).where($"ORIGIN_COUNTRY_NAME" =!= "United States")
    println(df_filtered.count())





    spark.stop()
  }
}
// scalastyle:on println
