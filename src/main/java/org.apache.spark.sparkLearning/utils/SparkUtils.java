package org.apache.spark.sparkLearning.utils;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * spark工具类
 * 
 * @author dlm
 *
 */
public class SparkUtils {

	/**
	 * 获取JavaSparkContext
	 * 
	 * @return
	 */
	public static JavaSparkContext getJavaSparkContext(String appName, String master,
			String logLeverl) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(appName).setMaster(master);

		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		jsc.setLogLevel(logLeverl);

		return jsc;
	}
	
	/**
	 * 获取JavaSparkContext
	 * 
	 * @return
	 */
	public static JavaStreamingContext getJavaStreamingContext(String appName, String master,
			String logLeverl,Duration batchDuration) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName(appName).setMaster(master);
		
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,batchDuration);
		return jsc;
	}


	/**
	 * 外部存储方式读取RDD，文件读取
	 * 
	 * @param jsc
	 * @return
	 */
	public static JavaRDD<String> createRddExternal(JavaSparkContext jsc, String filePath) {
		if (jsc == null) {
			return null;
		}

		// 文件读取方式创建RDD
		JavaRDD<String> readmeRdd = jsc.textFile(filePath);

		return readmeRdd;

	}

	/**
	 * 集合方式创建RDD
	 * 
	 * @param jsc
	 * @return
	 */
	public static JavaRDD<Integer> createRddCollect(JavaSparkContext jsc, List<Integer> list) {
		if (jsc == null) {
			return null;
		}

		// 创建RDD
		JavaRDD<Integer> listRdd = jsc.parallelize(list);

		return listRdd;
	}

	/**
	 * 集合方式创建PairRDD
	 * 
	 * @param jsc
	 * @return
	 */
	public static JavaPairRDD<Integer, Integer> createPairRddCollect(JavaSparkContext jsc,
			List<Tuple2<Integer, Integer>> list) {
		if (jsc == null) {
			return null;
		}

		// 创建RDD
		JavaPairRDD<Integer, Integer> pairRDD = jsc.parallelizePairs(list);

		return pairRDD;
	}
}
