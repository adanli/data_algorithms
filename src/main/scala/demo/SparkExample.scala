package demo

import config.SparkConfig
import org.apache.spark.sql.functions.col

object SparkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.spark("spark-example")

    /*val strings = spark.read.textFile("s3a://books/README.md")
    strings.show(10, truncate = false)

    println(strings.filter(col("value").contains("Spark")).count())*/


    val claim = spark.read.option("header", "true").csv("s3a://data/系统清单数据/案件清单-20231008.csv")
    claim.show(false)

    val s1 = System.currentTimeMillis()
    println(claim.count())
    println(s"案件量统计耗时: ${System.currentTimeMillis() - s1}")

    val claimByPolicy = spark.read.options(Map("header" -> "true")).csv("s3a://data/系统清单数据/赔案清单-20231008.csv")
    claimByPolicy.show(false)

    val s2 = System.currentTimeMillis()
    println(claimByPolicy.count())
    println(s"赔案量统计耗时: ${System.currentTimeMillis() - s2}")

  }

}
