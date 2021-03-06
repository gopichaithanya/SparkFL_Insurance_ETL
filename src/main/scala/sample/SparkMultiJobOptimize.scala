package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


case class SimpsonCharacter(name:String,actor:String,episodeDebut:String)
object SparkMultiJobOptimize  extends App{

  val spark =SparkSession.builder().appName("spark_App")
    .master("local[3]").getOrCreate()
  import spark.implicits._

  val employee =spark.range(0,100).select($"id".as("employee_id"),(rand()*3).cast("int").as("dep_id"),(rand()*40+20).cast("int").as("age")).toDF()
  val simpsonsDF = spark.sparkContext.parallelize(
    SimpsonCharacter("Homer", "Dan Castellaneta", "Good Night") ::
      SimpsonCharacter("Marge", "Julie Kavner", "Good Night") ::
      SimpsonCharacter("Bart", "Nancy Cartwright", "Good Night") ::
      SimpsonCharacter("Lisa", "Yeardley Smith", "Good Night") ::
      SimpsonCharacter("Maggie", "Liz Georges and more", "Good Night") ::
      SimpsonCharacter("Sideshow Bob", "Kelsey Grammer", "The Telltale Head") ::
      Nil)

  for (a <- 0 until 20) {
    val thread = new Thread {
      override def run {
        spark.sparkContext.parallelize(Array("aaa", "bbb", "ccc")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file1")
        spark.sparkContext.parallelize(Array("ddd", "eee", "fff")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file2")
        spark.sparkContext.parallelize(Array("ggg", "hhh", "iii")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file3")
        spark.sparkContext.parallelize(Array("jjj", "kkk", "lll")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file4")
        spark.sparkContext.parallelize(Array("mmm", "nnn", "ooo")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file5")
        spark.sparkContext.parallelize(Array("ppp", "qqq", "rrr")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file6")
        spark.sparkContext.parallelize(Array("sss", "ttt", "uuu")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file7")
        spark.sparkContext.parallelize(Array("aaa", "bbb", "ccc")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file8")
        spark.sparkContext.parallelize(Array("ddd", "eee", "fff")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file9")
        spark.sparkContext.parallelize(Array("ggg", "hhh", "iii")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file10")
        spark.sparkContext.parallelize(Array("jjj", "kkk", "lll")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file11")
        spark.sparkContext.parallelize(Array("mmm", "nnn", "ooo")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file12")
        spark.sparkContext.parallelize(Array("ppp", "qqq", "rrr")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file13")
        spark.sparkContext.parallelize(Array("sss", "ttt", "uuu")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file14")
        simpsonsDF.toDF().write.mode("overwrite").save("/tmp/vgiri/simpsonsDF_files")
        employee.createOrReplaceTempView("employee")
        spark.table("employee").write.mode("overWrite").format("parquet").saveAsTable("emp")
      }

    }
    thread.start
  }




}
