package org.apache.spark.examples

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Description:
  *
  * <p></p>
  * @author jsen.yin [jsen.yin@gmail.com]
  *         2019-02-19
  */
object JDBCDataSource {

  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession
    val spark: SparkSession = SparkSession.builder().appName(s"${this.getClass.getName}").master("local[*]").getOrCreate()
    // 2.读取数据库中的数据
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
    var properties = new Properties()
    properties.load(is)
    val url = "jdbc:mysql://47.98.187.132:3316/nqbasic0926?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&useSSL=false"

    val df: DataFrame = spark.read.jdbc(url, "`order`", properties)
    // 3.将数据输出目标中
    df.createTempView("order")
    val dfRes: DataFrame = spark.sql("select * from order where create_time > '2019-01-01 00:00:00'")
    // 3.1将数据写入到数据库中
    //        dfRes.write.jdbc("jdbc:mysql://localhost:3307/demo?characterEncoding=utf-8", "bigdata1", properties)
    // 3.2将数据写入到text文件中
    //        dfRes.write.text("D:/logs/test/eee")
    // 3.3将数据写入到json文件中
    //        dfRes.write.json("D:/logs/test/fff")
    // 3.4将数据写入到CSV文件中
            dfRes.write.csv("/mnt/workspace/hadoop/spark/spark-skew-join-examples/data/nyse/order.csv")
    // 3.5将数据写入到paruet文件中
    //    dfRes.write.parquet("/mnt/workspace/hadoop/spark/spark-skew-join-examples/data/nyse/order.csv")

    // 4.展示数据
    dfRes.show()
    // 5.释放资源
    spark.stop()
  }
}