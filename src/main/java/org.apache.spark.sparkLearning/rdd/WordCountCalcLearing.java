package org.apache.spark.sparkLearning.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sparkLearning.utils.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * word文件单词出现个数计算
 * 
 * @author dlm
 *
 */
public class WordCountCalcLearing {

	private static Logger logger = LoggerFactory.getLogger(WordCountCalcLearing.class);

	public static void main(String[] args) {
		JavaSparkContext jsc = SparkUtils.getJavaSparkContext("WordCountSpark", "local[2]", "WARN");

		JavaRDD<String> wordRdd = SparkUtils.createRddExternal(jsc, "/home/hadoop/text/aaaaaaaa.txt");

		wordCountCal(wordRdd);
		
		jsc.stop();

	}

	/**
	 * wordRdd统计计算逻辑
	 * 
	 * @param wordRdd
	 */
	public static void wordCountCal(JavaRDD<String> wordRdd) {
		// 将整个字符串根据空格分隔成单词
		JavaRDD<String> wordFlatMap = wordRdd.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("[^a-zA-Z']+")).iterator();
			}
		});

		// 将每个单词映射各位为1
		JavaPairRDD<String, Integer> wordMapToPair = wordFlatMap.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});

		// 将每个重复key的value相加
		JavaPairRDD<String, Integer> wordReduceByKey = wordMapToPair
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});

		// 输出统计结果
		wordReduceByKey.sortByKey().foreach(new VoidFunction<Tuple2<String, Integer>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				logger.warn("key:" + t._1 + ",value:" + t._2);
			}
		});
	}
}
