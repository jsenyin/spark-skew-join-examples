package org.apache.spark.sparkLearning.rdd;

import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sparkLearning.utils.SparkUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Example of using Spark's status APIs from Java.
 */
public final class JavaStatusTrackerDemo {
	private static Logger logger = LoggerFactory
			.getLogger(JavaStatusTrackerDemo.class);

	public static final class IdentityWithDelay<T> implements Function<T, T> {
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public T call(T x) throws Exception {
			Thread.sleep(2 * 1000); // 2 seconds
			return x;
		}
	}

	public static void main(String[] args) throws Exception {
		final JavaSparkContext sc = SparkUtils
				.getJavaSparkContext("JavaStatusAPIDemo", "local[2]", "WARN");

		// Example of implementing a progress reporter for a simple job.
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5)
				.map(new IdentityWithDelay<Integer>());
		JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
		while (!jobFuture.isDone()) {
			Thread.sleep(1000); // 1 second
			List<Integer> jobIds = jobFuture.jobIds();
			if (jobIds.isEmpty()) {
				continue;
			}
			int currentJobId = jobIds.get(jobIds.size() - 1);
			SparkJobInfo jobInfo = sc.statusTracker().getJobInfo(currentJobId);
			SparkStageInfo stageInfo = sc.statusTracker()
					.getStageInfo(jobInfo.stageIds()[0]);
			logger.warn(stageInfo.numTasks() + " tasks total: "
					+ stageInfo.numActiveTasks() + " active, "
					+ stageInfo.numCompletedTasks() + " complete");
		}

		logger.warn("Job results are: " + jobFuture.get());
		sc.stop();
	}
}