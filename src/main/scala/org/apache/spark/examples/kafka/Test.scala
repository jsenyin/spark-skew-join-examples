package org.apache.spark.examples.kafka

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * @Description:
      *
  * <p></p>
  * @author jsen.yin [jsen.yin@gmail.com]
  *         2019-01-26
  */
object Test {

  def leftJoin(left:RDD[(String, String)],right:RDD[(String, String)])={
    left.mapValues(x=>List((x,0))).union(right.mapValues(x=>List((x,1))))
      .reduceByKey(_:::_).flatMapValues{x=>
      val l=x.filter(_._2==0)
      val r=x.filter(_._2==1)
      if(l.isEmpty){
        List()
      }else if(r.isEmpty){
        l.map{t=>(t._1,None)}
      }else{
        l.map{t=>(t._1,Some(r.head._1))}
      }
    }.map{case(key,(v1,v2))=>
      (key,v1,v2)
    }
  }

  def join(left:RDD[(String,String)],right:RDD[(String,String)])={
    left.mapValues(x=>List((x,0))).union(right.mapValues(x=>List((x,1))))
      .reduceByKey(_:::_).flatMapValues{x=>
      val l=x.filter(_._2==0)
      val r=x.filter(_._2==1)
      if(l.isEmpty || r.isEmpty){
        List()
      }else{
        l.map{t=>(t._1,r.head._1)}
      }
    }.map{case(key,(v1,v2))=>
      (key,v1,v2)
    }
  }

  def leftJoin[K:ClassTag,V:ClassTag](left:RDD[(K, V)],right:RDD[(K, V)])={
    left.mapValues(x=>List((x,0))).union(right.mapValues(x=>List((x,1))))
      .reduceByKey(_:::_)
      .flatMapValues{x=>
        val l=x.filter(_._2==0)
        val r=x.filter(_._2==1)
        if(l.isEmpty){
          List()
        }else if(r.isEmpty){
          l.map{t=>(t._1,None)}
        }else{
          l.map{t=>(t._1,Some(r.head._1))}
        }
      }
  }

  def join[K:ClassTag,V:ClassTag](left:RDD[(K,V)],right:RDD[(K,V)])={
    left.mapValues(x=>List((x,0))).union(right.mapValues(x=>List((x,1))))
      .reduceByKey(_:::_)
      .flatMapValues{x=>
        val l=x.filter(_._2==0)
        val r=x.filter(_._2==1)
        if(l.isEmpty || r.isEmpty){
          List()
        }else{
          l.map{t=>(t._1,r.head._1)}
        }
      }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("JavaJsonTest")
    sparkConf.setMaster("spark://Nq011:7077")
    val sc = new SparkContext(sparkConf)

    val collect = sc.textFile("hdfs://nq011:9000/wc/java_error_in_IDEA_3126.log").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).collect

    print(collect)
//    val ds = sqlContext.read.text("hdfs://bsr.cn:9000/wc").as[String]
//    val result = ds.flatMap(_.split(" "))toDF().groupBy($"value" as “word”).agg(count("*") as "numOccurances").orderBy($"numOccurances" desc)
//    val df1 = spark.createDataset(Seq(("aaa", 1, 2), ("bbb", 3, 4), ("ccc", 3, 5), ("bbb", 4, 6)) ).toDF("key1","key2","key3")


  }


}
