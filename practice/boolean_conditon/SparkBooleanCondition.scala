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

    val DOTCodeFilter = col("StockCode") === "DOT"
    val priceFilter = col("UnitPrice") > 600
    val descriptionFilter = col("Description").contains("POSTAGE")

    val df_filtered = df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter)))
      .where(col("isExpensive") === true)
      .select("unitPrice", "StockCode", "Quantity")

    spark.stop()
  }
}
// scalastyle:on println
