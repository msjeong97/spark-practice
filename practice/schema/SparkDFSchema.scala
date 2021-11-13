// scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, DoubleType, LongType, StringType}
import org.apache.spark.sql.types.Metadata

object SparkDFSchema extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkDFSchema")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val flightData_1 = spark.read
      .format("json")
      .load("/Users/minseop/minseop/spark/data/flight-data/json/2015-summary.json")

    // StructType(
    //   StructField(DEST_COUNTRY_NAME,StringType,true),
    //   StructField(ORIGIN_COUNTRY_NAME,StringType,true),
    //   StructField(count,LongType,true)
    // )
    println(flightData_1.schema)

    // column name, type, nullable
    val definedSchema = StructType(Array(
      StructField("DEST_COUNTRY_NAME_defined", StringType, true),
      StructField("ORIGIN_COUNTRY_NAME_defined", StringType, true),
      StructField("count_defined", DoubleType, true)
    ))

    val flightData_2 = spark.read
      .format("json")
      .schema(definedSchema)
      .load("/Users/minseop/minseop/spark/data/flight-data/json/2015-summary.json")

    println(flightData_2.schema)

    spark.stop()
  }
}
// scalastyle:on println
