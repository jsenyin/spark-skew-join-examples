package org.apache.spark.sparkLearning.orderexmaple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.spark.sparkLearning.utils.ConstantUtils;
import org.apache.spark.sparkLearning.utils.SparkUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * spark streaming统计订单量和订单总值
 * 
 * @author liangming.deng
 *
 */
public class OrderSparkStreaming {
	private static Logger logger = LoggerFactory.getLogger(OrderSparkStreaming.class);
	private static AtomicLong orderCount = new AtomicLong(0);
	private static AtomicDouble totalPrice = new AtomicDouble(0);

	public static void main(String[] args) throws InterruptedException {

		// Create context with a 2 seconds batch interval
		JavaStreamingContext jssc = SparkUtils.getJavaStreamingContext("JavaDirectKafkaWordCount",
				"local[2]", null, Durations.seconds(20));

		Set<String> topicsSet = new HashSet<>(Arrays.asList(ConstantUtils.ORDER_TOPIC.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", ConstantUtils.METADATA_BROKER_LIST_VALUE);
		kafkaParams.put("auto.offset.reset", ConstantUtils.AUTO_OFFSET_RESET_VALUE);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> orderMsgStream = KafkaUtils.createDirectStream(jssc,
				String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
				topicsSet);

		// json与对象映射对象
		final ObjectMapper mapper = new ObjectMapper();
		JavaDStream<Order> orderDStream = orderMsgStream
				.map(new Function<Tuple2<String, String>, Order>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Order call(Tuple2<String, String> t2) throws Exception {
						Order order = mapper.readValue(t2._2, Order.class);
						return order;
					}
				}).cache();

		// 对DStream中的每一个RDD进行操作
		orderDStream.foreachRDD(new VoidFunction<JavaRDD<Order>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Order> orderJavaRDD) throws Exception {
				long count = orderJavaRDD.count();
				if (count > 0) {
					// 累加订单总数
					orderCount.addAndGet(count);
					// 对RDD中的每一个订单，首先进行一次Map操作，产生一个包含了每笔订单的价格的新的RDD
					// 然后对新的RDD进行一次Reduce操作，计算出这个RDD中所有订单的价格众合
					Float sumPrice = orderJavaRDD.map(new Function<Order, Float>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Float call(Order order) throws Exception {
							return order.getPrice();
						}
					}).reduce(new Function2<Float, Float, Float>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Float call(Float a, Float b) throws Exception {
							return a + b;
						}
					});
					// 然后把本次RDD中所有订单的价格总和累加到之前所有订单的价格总和中。
					totalPrice.getAndAdd(sumPrice);

					// 数据订单总数和价格总和，生产环境中可以写入数据库
					logger.warn("-------Total order count : " + orderCount.get()
							+ " with total price : " + totalPrice.get());
				}
			}
		});
		orderDStream.print();

		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
	}
}
