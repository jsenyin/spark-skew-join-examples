package org.apache.spark.sparkLearning.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sparkLearning.utils.SparkUtils;

import scala.Tuple2;

public class PairRDDLearning {
	private static Logger logger = LoggerFactory.getLogger(PairRDDLearning.class);

	public static void main(String[] args) {

		JavaSparkContext jsc = SparkUtils.getJavaSparkContext("PairLearning", "local[2]", "WARN");

		// 单个集合转化操作
		singleOperatePairRdd(jsc);
		// 多个集合转化操作
		multiOperatePairRdd(jsc); // 单个集合执行操作
		singleActionPairRdd(jsc);

		jsc.stop();

		logger.warn("the program end");

	}

	/**
	 * 单个转化操作 {(1, 2), (3, 4), (3, 6)}
	 * 
	 * @param jsc
	 */
	public static void singleOperatePairRdd(JavaSparkContext jsc) {

		// 初始化list
		List<Tuple2<Integer, Integer>> list = new ArrayList<>();
		Tuple2<Integer, Integer> tuple1 = new Tuple2<Integer, Integer>(1, 2);
		Tuple2<Integer, Integer> tuple2 = new Tuple2<Integer, Integer>(3, 4);
		Tuple2<Integer, Integer> tuple3 = new Tuple2<Integer, Integer>(3, 6);
		list.add(tuple1);
		list.add(tuple2);
		list.add(tuple3);

		// 创建pairRDD
		JavaPairRDD<Integer, Integer> numsPairRdd = SparkUtils.createPairRddCollect(jsc, list);

		// reduceByKey
		JavaPairRDD<Integer, Integer> numsReduceByKey = numsPairRdd
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

		logger.warn("PairRDD ReduceByKey:" + numsReduceByKey.collect().toString());

		// groupByKey()
		JavaPairRDD<Integer, Iterable<Integer>> numsGroupByKey = numsPairRdd.groupByKey();
		logger.warn("PairRDD ReduceByKey:" + numsGroupByKey.collect().toString());

		// combineByKey
		JavaPairRDD<Integer, Integer> numsCombineByKey = numsPairRdd
				.combineByKey(new Function<Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1) throws Exception {
						// TODO Auto-generated method stub
						return v1 + 1;
					}
				}, new Function2<Integer, Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				}, new Function2<Integer, Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						return v1 + v2;
					}
				});

		logger.warn("PairRDD numsCombineByKey:" + numsCombineByKey.collect().toString());

		// mapVavlues
		JavaPairRDD<Integer, Integer> numsMapValues = numsPairRdd
				.mapValues(new Function<Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1) throws Exception {
						// TODO Auto-generated method stub
						return v1 + 1;
					}
				});

		logger.warn("PairRDD numsMapValues:" + numsMapValues.collect().toString());

		// flatMapvlues
		JavaPairRDD<Integer, Integer> numsFlatMapValues = numsPairRdd
				.flatMapValues(new Function<Integer, Iterable<Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Integer> call(Integer v1) throws Exception {
						return Arrays.asList(v1 + 5);
					}
				});

		logger.warn("PairRDD numsFlatMapValues:" + numsFlatMapValues.collect().toString());

		// keys
		logger.warn("PairRDD numskeys:" + numsPairRdd.keys().collect().toString());

		// values
		logger.warn("PairRDD numsValues:" + numsPairRdd.values().collect().toString());

		// sortByKey
		logger.warn("PairRDD numsSortByKey:" + numsPairRdd.sortByKey().collect().toString());

	}

	/**
	 * 多个转化操作
	 * 
	 * @param jsc
	 */
	public static void multiOperatePairRdd(JavaSparkContext jsc) {

		// 初始化list
		List<Tuple2<Integer, Integer>> list = new ArrayList<>();
		Tuple2<Integer, Integer> tuple1 = new Tuple2<Integer, Integer>(1, 2);
		Tuple2<Integer, Integer> tuple2 = new Tuple2<Integer, Integer>(3, 4);
		Tuple2<Integer, Integer> tuple3 = new Tuple2<Integer, Integer>(3, 6);
		list.add(tuple1);
		list.add(tuple2);
		list.add(tuple3);

		// 创建pairRDD
		JavaPairRDD<Integer, Integer> oneNumsPairRdd = SparkUtils.createPairRddCollect(jsc, list);

		List<Tuple2<Integer, Integer>> list2 = new ArrayList<>();
		Tuple2<Integer, Integer> tuple4 = new Tuple2<Integer, Integer>(3, 9);
		list2.add(tuple4);

		// 创建pairRDD
		JavaPairRDD<Integer, Integer> twoNumsPairRdd = SparkUtils.createPairRddCollect(jsc, list2);

		// subtractByKey
		JavaPairRDD<Integer, Integer> numsSubtractByKey = oneNumsPairRdd
				.subtractByKey(twoNumsPairRdd);

		logger.warn(
				"multiOperatePairRdd numsSubtractByKey->" + numsSubtractByKey.collect().toString());

		// join
		logger.warn("multiOperatePairRdd join->"
				+ oneNumsPairRdd.join(twoNumsPairRdd).collect().toString());

		// rightOuterJoin
		logger.warn("multiOperatePairRdd rightOuterJoin->"
				+ oneNumsPairRdd.rightOuterJoin(twoNumsPairRdd).collect().toString());

		// leftOuterJoin
		logger.warn("multiOperatePairRdd leftOuterJoin->"
				+ oneNumsPairRdd.leftOuterJoin(twoNumsPairRdd).collect().toString());

		// cogroup
		logger.warn("multiOperatePairRdd cogroup->"
				+ oneNumsPairRdd.cogroup(twoNumsPairRdd).collect().toString());

	}

	/**
	 * 单个执行操作
	 * 
	 * @param jsc
	 */
	public static void singleActionPairRdd(JavaSparkContext jsc) {
		// 初始化list
		List<Tuple2<Integer, Integer>> list = new ArrayList<>();
		Tuple2<Integer, Integer> tuple1 = new Tuple2<Integer, Integer>(1, 2);
		Tuple2<Integer, Integer> tuple2 = new Tuple2<Integer, Integer>(3, 4);
		Tuple2<Integer, Integer> tuple3 = new Tuple2<Integer, Integer>(3, 6);
		list.add(tuple1);
		list.add(tuple2);
		list.add(tuple3);
		// 创建pairRDD
		JavaPairRDD<Integer, Integer> numsPairRdd = SparkUtils.createPairRddCollect(jsc, list);

		// countByKey()
		logger.warn("singleActionPairRdd countByKey->" + numsPairRdd.countByKey().toString());

		// collectAsMap()
		logger.warn("singleActionPairRdd collectAsMap()->" + numsPairRdd.collectAsMap().toString());

		// lookup
		logger.warn("singleActionPairRdd lookup->" + numsPairRdd.lookup(3).toString());
	}

}
