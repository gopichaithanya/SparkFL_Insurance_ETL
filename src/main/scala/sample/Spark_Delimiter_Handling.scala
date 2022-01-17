package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark_Delimiter_Handling extends App{

  val spark = SparkSession.builder.master("local").appName("spd").getOrCreate()
  import spark.implicits._
  val employee = spark.range(0,100).select($"id".as("employee_id"),(rand()*3).cast("int").as("dep_id"),(rand() * 40+20).cast("int").as("age"))
  employee.createOrReplaceTempView("hive001")
  spark.sql("select concat_ws('\001',employee_id,dep_id,age) as allnew from hive001").repartition(2).write.mode("overwrite").format("text").saveAsTable("hive001_new")
  spark.sql("select concat_ws('\001',employee_id,dep_id,age) as allnew from hive001").repartition(2).write.mode("overwrite").format("text").save("/hive001_new")
  spark.sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\001").load("spark-warehouse/hive001_new").show(3,false)
  spark.sparkContext.textFile("spark-warehouse/hive001_new").map(_.split("\001")).take(3)
  spark.sparkContext.textFile("spark-warehouse/hive001_new").map(_.split("\001")).take(3)

}
