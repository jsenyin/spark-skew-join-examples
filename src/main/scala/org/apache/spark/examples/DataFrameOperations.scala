package org.apache.spark.examples

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Description:
  *
  * <p></p>
  * @author jsen.yin [jsen.yin@gmail.com]
  *         2019-02-19
  */
object DataFrameOperations {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("LoadJDBCTest")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark SQL DataFrame Operations").setMaster("local[2]")
    val sparkContext: SparkContext = spark.sparkContext
    //new SparkContext(sparkConf)
    val sqlContext: SQLContext = spark.sqlContext
    //new SQLContext(sparkContext)
    val url = "jdbc:mysql://47.98.187.132:3316/nqbasic0926?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true&useSSL=false"


    val jdbcDF_buried_point_sku_record: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "buried_point_sku_record")).load()
    jdbcDF_buried_point_sku_record.createOrReplaceTempView("buried_point_sku_record")

    val joinDF_sku: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "sku")).load()
    joinDF_sku.createOrReplaceTempView("sku")

    val joinDF_order: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "`order`")).load()
    joinDF_order.createOrReplaceTempView("order")


    val joinDF_order_sku: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "order_sku")).load()
    joinDF_order_sku.createOrReplaceTempView("order_sku")

    val joinDF_order_after_sales: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "order_after_sales")).load()
    joinDF_order_after_sales.createOrReplaceTempView("order_after_sales")


    val joinDF_supplier_sku: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "supplier_sku")).load()
    joinDF_supplier_sku.createOrReplaceTempView("supplier_sku")


    val joinDF_inventory: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "inventory")).load()
    joinDF_inventory.createOrReplaceTempView("inventory")


    val joinDF_category: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "category")).load()
    joinDF_category.createOrReplaceTempView("category")


    val joinDF_brand: DataFrame = sqlContext.read.format("jdbc").options(
      Map("url" -> url,
        "user" -> "root",
        "password" -> "Nq123456!",
        "dbtable" -> "brand")).load()
    joinDF_brand.createOrReplaceTempView("brand")

    /*
        jdbcDF_buried_point_sku_record.show(1)
        joinDF_sku.show(1)
        joinDF_order.show(1)
        joinDF_order_sku.show(1)
        joinDF_order_after_sales.show(1)
        joinDF_supplier_sku.show(1)
        joinDF_inventory.show(1)
        joinDF_order.show(1)
        joinDF_category.show(1)
        joinDF_brand.show(1)
    */

    sqlContext.sql("set spark.sql.caseSensitive = true")

    /*
    val sqlDF = sqlContext.sql(
      "SELECT " +
        "       sku.id," +
        "       sku.sn," +
        "       sku.name," +
        "       sku.item_cat_id           As itemCatId," +
        "       ca.name                   AS itemCatName," +
        "       sku.is_specify_price      AS isSpecifyPrice," +
        "       sku.status," +
        "       sku.barcode," +
        "       sku.is_main_sku           AS isMainSku," +
        "       sku.sku_group_id          AS skuGroupId," +
        "       sku.brand_id              AS brandId," +
        "       b.name                    AS brandName," +
        "       sku.trade_type            AS tradeType," +
        "       sku.sku_prop              AS skuProp," +
        "       SUM(i.available_quantity) AS totalInventory" +
        "       FROM sku" +
        "       LEFT JOIN (SELECT sku_id, SUM(click_num) cnt" +
        "                  from buried_point_sku_record point" +
        "                  GROUP BY sku_id" +
        "                  HAVING cnt >= 100) t1 ON sku.id = t1.sku_id" +
        "       LEFT JOIN (SELECT distinct os.sku_id" +
        "                  FROM `order` o" +
        "                         join order_sku os ON o.order_num = os.order_num" +
        "                  WHERE 1 = 1" +
        "                    AND o.pay_status = 1" +
        "                    AND o.order_status BETWEEN 1 AND 6" +
        "                    AND o.create_time > '2019-01-01 00:00:00'" +
        "                    AND NOT EXISTS(SELECT 1" +
        "                                   FROM order_after_sales oas" +
        "                                   WHERE 1 = 1" +
        "                                     AND (oas.status BETWEEN 6 AND 8)" +
        "                                     AND (oas.order_status = 7)" +
        "                                     AND oas.order_num = o.order_num" +
        "                      )) t2 ON sku.id = t2.sku_id" +
        "       LEFT JOIN supplier_sku ss ON sku.id = ss.sku_id AND ss.sku_status = 0" +
        "       LEFT JOIN inventory i ON i.supplier_sku_id = ss.id" +
        "       LEFT JOIN category ca ON sku.item_cat_id = ca.id" +
        "       LEFT JOIN brand b ON sku.brand_id = b.id" +
        "  WHERE 1 = 1" +
        "  AND t1.sku_id IS NULL" +
        "  AND t2.sku_id IS NULL" +
        "  AND sku.status BETWEEN 1 AND 5" +
        "  GROUP BY id," +
        "       sn," +
        "       name," +
        "       itemCatId," +
        "       itemCatName," +
        "       isSpecifyPrice," +
        "       status," +
        "       barcode," +
        "       isMainSku," +
        "       skuGroupId," +
        "       brandId," +
        "       brandName," +
        "       tradeType," +
        "       skuProp" +
        " " +
        "")*/
    //    sqlDF.show()

    val DF_buried_point_sku_record: DataFrame = jdbcDF_buried_point_sku_record.where("click_num > 1")
    val DF_order: DataFrame = joinDF_order.where("create_time > '2019-01-01 00:00:00'")

    val df_t2: DataFrame =
    /*spark.sql("select SELECT distinct os.sku_id" +
    "                  FROM `order` o" +
    "                         join order_sku os ON o.order_num = os.order_num" +
    "                  WHERE 1 = 1" +
    "                    AND o.pay_status = 1" +
    "                    AND o.order_status BETWEEN 1 AND 6" +
    "                    AND o.create_time > '2019-01-01 00:00:00'" +
    "                    AND NOT EXISTS(SELECT 1" +
    "                                   FROM order_after_sales oas" +
    "                                   WHERE 1 = 1" +
    "                                     AND (oas.status BETWEEN 6 AND 8)" +
    "                                     AND (oas.order_status = 7)" +
    "                                     AND oas.order_num = o.order_num" +
    "                      )")
*/

      spark.sql("SELECT " +
        "       sku.id," +
        "       sku.sn," +
        "       sku.name," +
        "       sku.item_cat_id           As itemCatId," +
        "       sku.is_specify_price      AS isSpecifyPrice," +
        "       sku.status," +
        "       sku.barcode," +
        "       sku.is_main_sku           AS isMainSku," +
        "       sku.sku_group_id          AS skuGroupId," +
        "       sku.brand_id              AS brandId," +
        "       sku.trade_type            AS tradeType," +
        "       sku.sku_prop              AS skuProp" +
        "       FROM sku" +
        "       WHERE 1 = 1" +
        "       AND sku.status BETWEEN 1 AND 5" +
        "       GROUP BY id," +
        "       sn," +
        "       name," +
        "       itemCatId," +
        "       isSpecifyPrice," +
        "       status," +
        "       barcode," +
        "       isMainSku," +
        "       skuGroupId," +
        "       brandId," +
        "       tradeType," +
        "       skuProp" +
        "       ")
    df_t2.show()


  }


}
