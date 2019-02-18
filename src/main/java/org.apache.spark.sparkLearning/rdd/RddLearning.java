package org.apache.spark.sparkLearning.rdd;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sparkLearning.utils.SparkUtils;

/**
 * rdd转化和执行操作方法
 * @author dlm
 *
 */
public class RddLearning {

	private static Logger logger = LoggerFactory.getLogger(RddLearning.class);

	public static void main(String[] args) {

		JavaSparkContext jsc = SparkUtils.getJavaSparkContext("RDDLearning", "local[2]", "WARN");

		// 外部文件创建JavaRDD
		SparkUtils.createRddExternal(jsc, "/home/hadoop/text/aaaaaaaa.txt");
		// 单个集合转化操作
		singleOperateRdd(jsc);
		// 多个集合转化操作
		multiOperateRdd(jsc);
		// 单个集合执行操作
		singleActionRdd(jsc);

		jsc.stop();
		
		logger.warn("the program end");

	}

	/**
	 * 单个转化操作
	 * 
	 * @param jsc
	 */
	public static void singleOperateRdd(JavaSparkContext jsc) {

		// 初始化list
		List<Integer> nums = Arrays.asList(new Integer[] { 1, 2, 3, 3 });
		JavaRDD<Integer> numsRdd = SparkUtils.createRddCollect(jsc, nums);

		// map
		JavaRDD<Integer> mapRdd = numsRdd.map(new Function<Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1) throws Exception {
				return (v1 + 1);
			}
		});

		logger.warn("singleOperateRdd mapRdd->" + mapRdd.collect().toString());

		JavaRDD<Integer> flatMapRdd = numsRdd.flatMap(new FlatMapFunction<Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Integer> call(Integer t) throws Exception {
				return Arrays.asList(new Integer[] { 2, 3 }).iterator();
			}
		});

		logger.warn("singleOperateRdd flatMapRdd->" + flatMapRdd.collect().toString());

		JavaRDD<Integer> filterRdd = numsRdd.filter(new Function<Integer, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer v1) throws Exception {
				return v1 > 2;
			}
		});

		logger.warn("singleOperateRdd filterRdd->" + filterRdd.collect().toString());

		JavaRDD<Integer> distinctRdd = numsRdd.distinct();

		logger.warn("singleOperateRdd distinctRdd->" + distinctRdd.collect().toString());

		JavaRDD<Integer> sampleRdd = numsRdd.sample(false, 0.5);

		logger.warn("singleOperateRdd sampleRdd->" + sampleRdd.collect().toString());
	}

	/**
	 * 多个转化操作
	 * 
	 * @param jsc
	 */
	public static void multiOperateRdd(JavaSparkContext jsc) {

		// 初始化list
		List<Integer> oneNums = Arrays.asList(new Integer[] { 1, 2, 3 });
		JavaRDD<Integer> oneNumsRdd = SparkUtils.createRddCollect(jsc, oneNums);

		logger.warn("multiOperateRdd oneNumsRdd->" + oneNumsRdd.collect().toString());

		// 初始化list
		List<Integer> twoNums = Arrays.asList(new Integer[] { 3, 4, 5 });
		JavaRDD<Integer> twoNumsRdd = SparkUtils.createRddCollect(jsc, twoNums);

		logger.warn("multiOperateRdd twoNumsRdd->" + twoNumsRdd.collect().toString());

		JavaRDD<Integer> unionRdd = oneNumsRdd.union(twoNumsRdd);

		logger.warn("multiOperateRdd unionRdd->" + unionRdd.collect().toString());

		JavaRDD<Integer> intersectionRdd = oneNumsRdd.intersection(twoNumsRdd);

		logger.warn("multiOperateRdd intersectionRdd->" + intersectionRdd.collect().toString());

		JavaRDD<Integer> subtractRdd = oneNumsRdd.subtract(twoNumsRdd);

		logger.warn("multiOperateRdd subtractRdd->" + subtractRdd.collect().toString());

		JavaPairRDD<Integer, Integer> cartesianPairRdd = oneNumsRdd.cartesian(twoNumsRdd);

		logger.warn("multiOperateRdd cartesianPairRdd->" + cartesianPairRdd.collect().toString());

	}

	/**
	 * 单个执行操作
	 * 
	 * @param jsc
	 */
	public static void singleActionRdd(JavaSparkContext jsc) {
		// 初始化list
		List<Integer> nums = Arrays.asList(new Integer[] { 1, 2, 2, 3, 3 });
		JavaRDD<Integer> numsRdd = SparkUtils.createRddCollect(jsc, nums);

		// collect
		logger.warn("singleActionRdd numsCollectRdd->" + numsRdd.collect().toString());

		// count
		logger.warn("singleActionRdd numsCountRdd->" + numsRdd.count());

		// countByValue
		logger.warn("singleActionRdd numsCountByValueRdd->" + numsRdd.countByValue().toString());

		// take
		logger.warn("singleActionRdd numsTakeRdd->" + numsRdd.take(2).toString());
		// top
		logger.warn("singleActionRdd numsTopRdd->" + numsRdd.top(2).toString());
		// takeOrdered
		logger.warn("singleActionRdd numsTakeOrderedRdd->" + numsRdd.takeOrdered(2).toString());
		// takeSample

		logger.warn("singleActionRdd numsTakeSampleRdd->" + numsRdd.takeSample(false, 1, 1L).toString());

		// reduce
		Integer valReduce = numsRdd.reduce(new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		logger.warn("singleActionRdd valReduce->" + valReduce);

		// fold,集合中所有元素+初始值,重复元素只加一次
		Integer valFold = numsRdd.fold(1, new Function2<Integer, Integer, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		logger.warn("singleActionRdd valFold->" + valFold);
		// aggregate,集合中所有元素+初始值,重复元素只加一次
		Integer valAggregate = numsRdd.aggregate(2, new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}, new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		logger.warn("singleActionRdd valAggregate->" + valAggregate);
		// foreach
		numsRdd.foreach(new VoidFunction<Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				logger.warn("singleActionRdd numsForeachRdd->" + t);

			}
		});

	}

}
