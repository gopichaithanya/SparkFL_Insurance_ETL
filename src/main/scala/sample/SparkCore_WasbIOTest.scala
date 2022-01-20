package sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Test IO to wasb
  */
object SparkCore_WasbIOTest {
  def main (arg: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCore_WasbIOTest").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("/data/_default_/HdiSamples/HdiSamples/SensorSampleData/hvac/HVAC.csv")

    // find the rows which have only one digit in the 6th column
    val rdd1 = rdd.filter(s => s.split(",")(6).length() == 1)

    rdd1.saveAsTextFile("HVACout2")
  }
}