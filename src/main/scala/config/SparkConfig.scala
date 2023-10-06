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
      .setMaster("spark://localhost:7077")
      .set("spark.executor.memory", "4g")
      .set("spark.executor.cores", "1")
      .set("HADOOP_USER_NAME", "root")
      .set("spark.driver.host", "localhost")
//      .set("spark.driver.host", "11.0.0.193")
      .set("spark.hadoop.fs.s3a.endpoint", "http://11.0.0.197:9000")
      .set("spark.hadoop.fs.s3a.access.key", "zb38TwMyPsi3aGigkcfT")
      .set("spark.hadoop.fs.s3a.secret.key", "ek7RlhyzafYLkb1YPIEgmfNRxIlF5FqHxVlJ9ZpV")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

//      .set("hadoop.home.dir", "D:/application/hadoop")
//      .set("HADOOP_HOME", "D:/application/hadoop")

      .setJars(Seq(
        "/Users/adan/code/egg/data_algorithms/target/scala-2.13/data_algorithms_2.13-1.0.jar",
//        "D:\\code\\spark-demo\\practice_spark\\target\\scala-2.13\\practice_spark_2.13-1.0.jar",
      ))
  }

}
