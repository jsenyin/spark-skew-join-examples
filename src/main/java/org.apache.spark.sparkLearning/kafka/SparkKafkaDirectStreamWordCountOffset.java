
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sparkLearning.kafka;

import java.util.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import org.apache.spark.sparkLearning.utils.SparkUtils;

import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 * <p>
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 * <p>
 * 统计kafka发送数据字符串个数
 */

public final class SparkKafkaDirectStreamWordCountOffset {
    private static final Pattern SPACE = Pattern.compile(" ");

    private static final String brokers = "10.32.19.41:9092,10.32.19.42:9092,10.32.19.43:9092";
    private static final String topics = "topTest1,topTest2";

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {

        // Create context with a 2 seconds batch interval
        JavaStreamingContext jssc = SparkUtils.getJavaStreamingContext("JavaDirectKafkaWordCount",
                "local[2]", null, Durations.seconds(10));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("auto.offset.reset", "smallest");

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams,
                topicsSet);

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        JavaPairDStream<String, String> msg = messages.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd)
                            throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                });


        /*msg.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
         *//**
         *
         *//*
			private static final long serialVersionUID = 5070375061056900677L;

			@Override
			public Void call(JavaPairRDD<String, String> rdd) throws IOException {
				for (OffsetRange o : offsetRanges.get()) {
					System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " "
							+ o.untilOffset());
				}
				return null;
			}
		});*/

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = msg.map(new Function<Tuple2<String, String>, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                // TODO Auto-generated method stub
                return v1._2;
            }
        });

        JavaDStream<String> words =
                lines.flatMap(new FlatMapFunction<String, String>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(String t) throws Exception {
                        // TODO Auto-generated method stub
                        return Arrays.asList(SPACE.split(t)).iterator();
                    }
                });
        JavaPairDStream<String, Integer> wordCounts =

                words.mapToPair(new PairFunction<String, String, Integer>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String t) throws Exception {
                        // TODO Auto-generated method stub
                        return new Tuple2<String, Integer>(t, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {

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
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
