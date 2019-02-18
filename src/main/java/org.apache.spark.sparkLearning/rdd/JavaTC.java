package org.apache.spark.sparkLearning.rdd;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sparkLearning.utils.SparkUtils;

/**
 * Transitive closure on a graph, implemented in Java. Usage: JavaTC [slices]
 */
public final class JavaTC {
	private static Logger logger = LoggerFactory.getLogger(JavaTC.class);

	private static final int numEdges = 20;
	private static final int numVertices = 100;
	private static final int slices = 2;
	private static final Random rand = new Random(42);

	static List<Tuple2<Integer, Integer>> generateGraph() {
		Set<Tuple2<Integer, Integer>> edges = new HashSet<Tuple2<Integer, Integer>>(numEdges);
		logger.warn("generateGraph start");
		while (edges.size() < numEdges) {
			int from = rand.nextInt(numVertices);
			int to = rand.nextInt(numVertices);
			Tuple2<Integer, Integer> e = new Tuple2<Integer, Integer>(from, to);
			if (from != to) {
				edges.add(e);
			}
		}

		return new ArrayList<Tuple2<Integer, Integer>>(edges);
	}

	static class ProjectFn
			implements PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>, Integer, Integer> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		static final ProjectFn INSTANCE = new ProjectFn();

		@Override
		public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> triple) {
			return new Tuple2<Integer, Integer>(triple._2()._2(), triple._2()._1());
		}
	}

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getJavaSparkContext("JavaTC", "local[2]", "WARN");
		// 获取两个坐标点
		JavaPairRDD<Integer, Integer> tc = sc.parallelizePairs(generateGraph(), slices).cache();

		// Linear transitive closure: each round grows paths by one edge,
		// by joining the graph's edges with the already-discovered paths.
		// e.g. join the path (y, z) from the TC with the edge (x, y) from
		// the graph to obtain the path (x, z).

		// Because join() joins on keys, the edges are stored in reversed order.
		// 坐标点位置置换
		JavaPairRDD<Integer, Integer> edges = tc
				.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> e) {
						return new Tuple2<Integer, Integer>(e._2(), e._1());
					}
				});

		long oldCount;
		long nextCount = tc.count();
		do {
			logger.warn("edges:" + edges.collect().toString());
			logger.warn("tc:" + tc.collect().toString());
			oldCount = nextCount;
			// Perform the join, obtaining an RDD of (y, (z, x)) pairs,
			// then project the result to obtain the new (x, z) paths.
			JavaPairRDD<Integer, Integer> tcJoin = tc.join(edges).mapToPair(ProjectFn.INSTANCE);
			logger.warn("tcJoin:" + tcJoin.collect().toString());
			tc = tc.union(tcJoin).distinct().cache();
			logger.warn("newTc:" + tc.collect().toString());
			nextCount = tc.count();
			logger.warn("while nextCount: " + tc.count() + ",oldCount:" + oldCount);
		} while (nextCount != oldCount);

		logger.warn("slices:" + slices + ". TC has " + tc.count() + " edges.");
		sc.stop();
	}
}
