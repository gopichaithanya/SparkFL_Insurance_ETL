package sample

import org.apache.spark.sql.SparkSession

object SparkETL_Vehicle_Data  extends App{

  val spark = SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()
  val SparkETL = spark.read.option("header","true").option("InferSchema","true").csv("data/FL_insurance_sample.csv")
  SparkETL.printSchema()
  SparkETL.show(10)
  SparkETL.createOrReplaceTempView("fl_insurance")
  val fl_results=spark.sql("select * from fl_insurance where statecode = 'FL'").show(5)
  val fl_agg=spark.sql("select policyID,max(eq_site_limit),county from fl_insurance group by policyID,county ").show(10)
  val fl_agg1 = spark.sql("select count(1) AS policyID ,max(eq_site_limit) AS highest_eq,county from fl_insurance group by county").repartition(4).write.mode("overwrite").format("parquet").save("VehicleETL_Results")
}
