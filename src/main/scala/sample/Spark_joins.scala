package sample

import org.apache.spark.sql.SparkSession

object Spark_joins extends App {

  val spark = SparkSession.builder.master("local[3]").appName("sparkA").getOrCreate()
  case class Book(book_name: String, cost: Int, writer_id:Int)
  import spark.implicits._
  val bookDS = Seq(
    Book("Scala", 400, 1),
    Book("Spark", 500, 2),
    Book("Kafka", 300, 3),
    Book("Java", 350, 5)
  ).toDS()
  bookDS.show()
  case class Writer(writer_name: String, writer_id:Int)
  val writerDS = Seq(
    Writer("Martin",1),
    Writer("Zaharia",2),
  Writer("Neha",3),
  Writer("James", 4)
  ).toDS()
  writerDS.show()
  val BookWriterInner = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "inner")
  BookWriterInner.show()

  spark.conf.set("spark.sql.crossJoin.enabled", true)
  val BookWriterCross = bookDS.join(writerDS)
  BookWriterCross.show()
  val BookWriterLeft = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "leftouter")
  BookWriterLeft.show()
  val BookWriterRight = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "rightouter")
  BookWriterRight.show()
  val BookWriterFull = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "fullouter")
  BookWriterFull.show()
  val BookWriterLeftSemi = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "leftsemi")
  BookWriterLeftSemi.show()

  val BookWriterLeftAnti = bookDS.join(writerDS, bookDS("writer_id") === writerDS("writer_id"), "leftanti")
  BookWriterLeftAnti.show()
}
