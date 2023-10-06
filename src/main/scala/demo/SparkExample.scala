package demo

import config.SparkConfig
import org.apache.spark.sql.functions.col

object SparkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.spark("spark-example")

    val strings = spark.read.textFile("s3a://books/README.md")
    strings.show(10, false)

    println(strings.filter(col("value").contains("Spark")).count())
  }

}
