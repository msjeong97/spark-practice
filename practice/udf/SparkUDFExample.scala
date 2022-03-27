// scalastyle:off println
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, udf, col}

object SparkUDFExample extends Serializable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SparkUDFExample")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5")

    val df = Seq(("Tom", 13, "A"), ("Billy", 15, "C"), ("Mason", 12, "B")).toDF("name", "age", "grade")
    df.show()

    val generateMessage = udf((grade: String) =>
      try {
        grade match {
          case "A" => "Good grade"
          case "B" => "Avergae grade"
          case "C" => "Poor grade"
        }
      } catch {
        case e: Exception => "No grade"
      }
    )

    val dfWithMessage = df.withColumn("message", generateMessage(col("grade")))
    dfWithMessage.show()

    // use udf with Spark SQL
    df.createOrReplaceTempView("grade_data")
    spark.udf.register("generateMessage", generateMessage)

    val grade_date = spark.sql("""
      SELECT name, age, grade, generateMessage(grade)
      FROM grade_data;
    """).show()


    spark.stop()
  }
}
// scalastyle:on println
