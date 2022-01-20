package sample

import org.apache.spark.sql.SparkSession

object Spark_ETL01  extends App{


  val spark = SparkSession.builder.master("local[3]").appName("spark-app").getOrCreate()
  println(spark)

}
