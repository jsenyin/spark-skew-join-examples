package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, DataFrameReader}
import java.sql.{Connection, DriverManager, ResultSet, Timestamp}
import scala.collection._


/**
  * @Description:
  *
  * <p></p>
  * @author jsen.yin [jsen.yin@gmail.com]
  *         2019-02-19
  */
case class TestPerson(name: String, age: Long, salary: Double)

import org.apache.spark.sql.SparkSession

object SqlRdd {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("LoadJDBCTest")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._

    val conf = spark.conf
    val sc = spark.sparkContext
    //    val conf = new SparkConf().setMaster("local").setAppName("LoadJDBCTest")
    //    val sc = new SparkContext(conf)
    val words = sc.textFile("/home/hadoop/0.0.filtertrie.intermediate.txt")
    println(words.count)

    val file = sc.textFile("/home/hadoop/dim_items.txt")
    print(file.first)

    //    print(words.map(l=>l.split("").size).filter(l=>l.contains("驱动器")).count)
    words.filter(l => l.trim() != "").map(l => l.split(" ").size).reduce((a, b) => Math.max(a, b))

    var tom = new TestPerson("Tom Hanks", 37, 35.5)
    var sam = new TestPerson("Sam Smith", 40, 40.5)

    val PersonList = mutable.MutableList[TestPerson]()

    PersonList += tom
    PersonList += sam

    val personDS = PersonList.toDS()
    personDS.show()

    val personDF = PersonList.toDF()
    println(personDF.getClass)
    personDF.show()
    personDF.select("name", "age").show()

  }
}