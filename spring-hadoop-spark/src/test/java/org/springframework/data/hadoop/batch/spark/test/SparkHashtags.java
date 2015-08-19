/*
 * Copyright 2014-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.batch.spark.test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkHashtags {

	private static ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

	public static void main(String[] args) {
		String fileName = "";
		if (args.length > 0) {
			fileName = args[0];
		}

		SparkConf conf = new SparkConf().setAppName("spark-hashtags");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> tweetData = sc.textFile(fileName).cache();

		JavaRDD<Map<String, Object>> tweets = tweetData.map(new Function<String, Map<String, Object>>() {
			public Map<String, Object> call(String s) throws Exception {
				return jsonMapper.readValue(s.toString(), new TypeReference<HashMap<String, Object>>() {
				});
			}
		});

		JavaPairRDD<String, Integer> hashTags = tweets.flatMapToPair(new PairFlatMapFunction<Map<String, Object>, String, Integer>() {
			public Iterable<Tuple2<String, Integer>> call(Map<String, Object> tweet) throws Exception {

				Map<String, Object> entities = (Map<String, Object>) tweet.get("entities");
				List<Map<String, Object>> hashTagEntries = null;
				if (entities != null) {
					hashTagEntries = (List<Map<String, Object>>) entities.get("hashtags");
				}
				List<Tuple2<String, Integer>> hashTags = new ArrayList<Tuple2<String, Integer>>();
				if (hashTagEntries != null && hashTagEntries.size() > 0) {
					for (Map<String, Object> hashTagEntry : hashTagEntries) {
						String hashTag = hashTagEntry.get("text").toString();
						hashTags.add(new Tuple2<String, Integer>(hashTag, 1));
					}
				}
				return hashTags;
			}
		});

		JavaPairRDD<String, Integer> hashTagCounts = hashTags.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer int1, Integer int2) throws Exception {
				return int1 + int2;
			}
		});

		JavaPairRDD<String, Integer> hashTagCountsSorted = hashTagCounts.mapToPair(
				new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> in) throws Exception {
						return new Tuple2<Integer, String>(in._2(), in._1());
					}
				}).sortByKey(false).mapToPair(
				new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					public Tuple2<String, Integer> call(Tuple2<Integer, String> in) throws Exception {
						return new Tuple2<String, Integer>(in._2(), in._1());
					}
				});

		List<Tuple2<String, Integer>> top10 = hashTagCountsSorted.take(10);
		System.out.println("HashTags: " + top10);

		JavaPairRDD<String, Integer> top10Hashtags = sc.parallelizePairs(top10);
		top10Hashtags.saveAsTextFile("hdfs:///test/spark/output");

		sc.stop();
	}

}
