package config

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfig {
  def spark(appName: String, conf: Option[SparkConf] = None): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .config(conf.getOrElse(config()))
      .getOrCreate()
  }

  def config(): SparkConf = {
    new SparkConf()
      .setAppName("data_algorithms")
      .setMaster(if(isMac) "spark://localhost:7077" else "spark://11.0.0.193:7077")
      .set("spark.executor.memory", "4g")
      .set("spark.executor.cores", "1")
      .set("HADOOP_USER_NAME", "root")
      .set("spark.driver.host", if(isMac) "localhost" else "11.0.0.199")
      .set("spark.hadoop.fs.s3a.endpoint", "http://11.0.0.197:9000")
      .set("spark.hadoop.fs.s3a.access.key", "nnICK7KOtzheuCh4aCW6")
      .set("spark.hadoop.fs.s3a.secret.key", "g8lPsriEGfpJaym0PMUYcEwnKBp8g4GOGWg5UU8B")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      .setJars(Seq(
        "/Users/adan/code/egg/data_algorithms/target/scala-2.13/data_algorithms_2.13-1.0.jar",
      ))
  }

  private def isMac: Boolean = {
    System.getProperty("os.name").contains("Mac")
  }

}
