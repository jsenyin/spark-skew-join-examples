package org.apache.spark.sparkLearning.orderexmaple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sparkLearning.utils.ConstantUtils;
import org.apache.spark.sparkLearning.utils.RandomUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 订单 kafka消息生产者
 * 
 * @author liangming.deng
 *
 */
public class OrderProducer {
	private static Logger logger = LoggerFactory.getLogger(OrderProducer.class);

	public static void main(String[] args) throws IOException {
		// set up the producer
		Producer<String, String> producer = null;
		ObjectMapper mapper = new ObjectMapper();

		try {

			Properties props = new Properties();
			// kafka集群
			props.put("metadata.broker.list", ConstantUtils.METADATA_BROKER_LIST_VALUE);

			// 配置value的序列化类
			props.put("serializer.class", ConstantUtils.SERIALIZER_CLASS_VALUE);
			// 配置key的序列化类
			props.put("key.serializer.class", ConstantUtils.SERIALIZER_CLASS_VALUE);

			ProducerConfig config = new ProducerConfig(props);
			producer = new Producer<String, String>(config);
			// 定义发布消息体
			List<KeyedMessage<String, String>> messages = new ArrayList<>();
			// 每隔3秒生产随机个订单消息
			while (true) {
				int random = RandomUtils.getRandomNum(20);
				if (random == 0) {
					continue;
				}
				messages.clear();
				for (int i = 0; i < random; i++) {
					int orderRandom = RandomUtils.getRandomNum(random * 10);
					Order order = new Order("name" + orderRandom, Float.valueOf("" + orderRandom));
					// 订单消息体:topic和消息
					KeyedMessage<String, String> message = new KeyedMessage<String, String>(
							ConstantUtils.ORDER_TOPIC, mapper.writeValueAsString(order));
					messages.add(message);
				}

				producer.send(messages);
				logger.warn("orderNum:" + random + ",message:" + messages.toString());
				Thread.sleep(10000);

			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("-------------:" + e.getStackTrace());
		} finally {
			producer.close();
		}

	}
}