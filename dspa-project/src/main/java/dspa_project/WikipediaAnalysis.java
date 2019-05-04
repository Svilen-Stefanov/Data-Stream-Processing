/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dspa_project;

import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import dspa_project.recommender_system.RecommenderSystem;
import dspa_project.stream.sources.SimulationSourceFunction;
import dspa_project.stream.operators.RecommendCommentAggregateFunction;
import dspa_project.stream.operators.RecommendLikeAggregateFunction;
import dspa_project.stream.operators.RecommendPostAggregateFunction;
import dspa_project.unusual_activity_detection.UnusualActivityDetection;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class WikipediaAnalysis {

	static int count = 0;
	static String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
	static String LOCAL_KAFKA_BROKER = "localhost:9092";
	static String GROUP = "";

	public static void main(String[] args) throws Exception {
		DataLoader dl = new DataLoader();
		parseArguments(args);

		/*
		 * ====================================================
		 * ====================================================
		 * ============== STATIC DATA ANALYSIS ================
		 * ====================================================
		 * ====================================================
		 * */

		RecommenderSystem recommenderSystem = new RecommenderSystem();
		UnusualActivityDetection uad = new UnusualActivityDetection();
		boolean checkCorrect = uad.checkLocation(122, 28);
		System.out.println(checkCorrect);
		checkCorrect = uad.checkLocation(919, 30);
		System.out.println(checkCorrect);
		System.exit(1);

		/*
		 * ====================================================
		 * ====================================================
		 * ============== STREAM DATA ANALYSIS ================
		 * ====================================================
		 * ====================================================
		 * */
		LikeEvent le = dl.parseLike();
		System.out.println(le.getId());
		System.out.println(le.getPersonId());
		System.out.println(le.getCreationDate());

		CommentEvent ce = dl.parseComment();

		System.out.println(ce.getContent());
		System.out.println(ce.getCreationDate());

		PostEvent pe = dl.parsePost();
		pe = dl.parsePost();
		System.out.println(pe.getContent());
		System.out.println(pe.getCreationDate());

		Properties props = new Properties();
		props.put("bootstrap.servers", LOCAL_KAFKA_BROKER);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", Class.forName("dspa_project.schemas.LikeSchema"));

		Producer<String, LikeEvent> likeProducer = new KafkaProducer<>(props);
		LikeEvent likeEvent = dl.parseLike();
		int i = 0;
		while (likeEvent != null) {
			if(i%10000 == 0)
				System.out.println("Like: " + i);
			i++;
			likeProducer.send(new ProducerRecord<String, LikeEvent>("like-topic", likeEvent));
			likeEvent = dl.parseLike();
		}

		likeProducer.close();

		props.put("value.serializer", Class.forName("dspa_project.schemas.CommentSchema"));
		Producer<String, CommentEvent> commentProducer = new KafkaProducer<>(props);
		CommentEvent commentEvent = dl.parseComment();
		i = 0;
		while (commentEvent != null) {
			if(i%10000 == 0)
				System.out.println("Comment: " + i);
			i++;
			commentProducer.send(new ProducerRecord<String, CommentEvent>("comment-topic", commentEvent));
			commentEvent = dl.parseComment();
		}

		commentProducer.close();

		props.put("value.serializer", Class.forName("dspa_project.schemas.PostSchema"));
		Producer<String, PostEvent> postProducer = new KafkaProducer<>(props);
		PostEvent postEvent = dl.parsePost();
		i = 0;
		while (postEvent != null) {
			if(i%10000 == 0)
				System.out.println("Post: " + i);
			i++;
			postProducer.send(new ProducerRecord<String, PostEvent>("post-topic", postEvent));
			postEvent = dl.parsePost();
		}

		postProducer.close();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// TODO: why do we take topic comment-topic and name the stream likes???
		SourceFunction<CommentEvent> source = new SimulationSourceFunction<CommentEvent>("comment-topic", "dspa_project.schemas.CommentSchema",
				                                                                  2, 10000, 10000);
		TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
		TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
		TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);
		DataStream<CommentEvent> comments = env.addSource(source, typeInfoComments);
		comments.print();
		env.execute("Flink Streaming Java API Skeleton");

		/*
		 * ====================================================
		 * ====================================================
		 * ================ RECOMMENDATIONS ===================
		 * ====================================================
		 * ====================================================
		 * */

		SourceFunction<LikeEvent> sourceRecommendationsLikes = new SimulationSourceFunction<LikeEvent>("like-topic", "dspa_project.schemas.LikeSchema",
				2, 10000, 10000);

		SourceFunction<CommentEvent> sourceRecommendationsComments = new SimulationSourceFunction<CommentEvent>("comment-topic", "dspa_project.schemas.CommentSchema",
				2, 10000, 10000);

		SourceFunction<PostEvent> sourceRecommendationsPosts = new SimulationSourceFunction<PostEvent>("post-topic", "dspa_project.schemas.PostSchema",
				2, 10000, 10000);

		DataStream<Tuple2<Long, Float>> recommendLikes = env.addSource(sourceRecommendationsLikes, typeInfoLikes)
				.keyBy(new KeySelector<LikeEvent, Long>() {
					@Override
					public Long getKey(LikeEvent likeEvent) throws Exception {
						return likeEvent.getPersonId();
					}
				})
				.window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
				.aggregate(new RecommendLikeAggregateFunction())
				.flatMap(new FlatMapFunction<HashMap<Long, Float>, Tuple2<Long, Float>>() {
					@Override
					public void flatMap(HashMap<Long, Float> longFloatHashMap, Collector<Tuple2<Long, Float>> collector) throws Exception {
						for (Map.Entry<Long, Float> entry : longFloatHashMap.entrySet()) {
							collector.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
						}
					}
				});

		//recommendLikes.print();

		DataStream<Tuple2<Long, Float>> recommendComments = env.addSource(sourceRecommendationsComments, typeInfoComments)
				.keyBy(new KeySelector<CommentEvent, Long>() {
					@Override
					public Long getKey(CommentEvent commentEvent) throws Exception {
						return commentEvent.getPersonId();
					}
				})
				.window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
				.aggregate(new RecommendCommentAggregateFunction())
				.flatMap(new FlatMapFunction<HashMap<Long, Float>, Tuple2<Long, Float>>() {
					@Override
					public void flatMap(HashMap<Long, Float> longFloatHashMap, Collector<Tuple2<Long, Float>> collector) throws Exception {
						for (Map.Entry<Long, Float> entry : longFloatHashMap.entrySet()) {
							collector.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
						}
					}
				});

		//recommendComments.print();

		// compute tips per hour for each driver
		DataStream<Tuple2<Long, Float>> recommendPosts = env.addSource(sourceRecommendationsPosts, typeInfoPosts)
				.keyBy(new KeySelector<PostEvent, Long>() {
					@Override
					public Long getKey(PostEvent postEvent) throws Exception {
						return postEvent.getPersonId();
					}
				})
				.window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
				.aggregate(new RecommendPostAggregateFunction())
				.flatMap(new FlatMapFunction<HashMap<Long, Float>, Tuple2<Long, Float>>() {
					@Override
					public void flatMap(HashMap<Long, Float> longFloatHashMap, Collector<Tuple2<Long, Float>> collector) throws Exception {
						for (Map.Entry<Long, Float> entry : longFloatHashMap.entrySet()) {
							collector.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
						}
					}
				});


		recommendLikes.join(recommendComments)
		.where(new KeySelector<Tuple2<Long, Float>, Long>() {
			@Override
			public Long getKey(Tuple2<Long, Float> longFloatTuple) throws Exception {
				return longFloatTuple.f0;
			}
		})
		.equalTo(new KeySelector<Tuple2<Long, Float>, Long>() {
			@Override
			public Long getKey(Tuple2<Long, Float> longFloatTuple) throws Exception {
				return longFloatTuple.f0;
			}
		})
		.window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
		.apply((longFloatTuple2, longFloatTuple22) -> new Tuple2<Long, Float>(longFloatTuple2.f0, longFloatTuple2.f1 + longFloatTuple22.f1))
				.join(recommendPosts)
				.where(new KeySelector<Tuple2<Long, Float>, Long>() {
					@Override
					public Long getKey(Tuple2<Long, Float> longFloatTuple) throws Exception {
						return longFloatTuple.f0;
					}
				})
				.equalTo(new KeySelector<Tuple2<Long, Float>, Long>() {
					@Override
					public Long getKey(Tuple2<Long, Float> longFloatTuple) throws Exception {
						return longFloatTuple.f0;
					}
				})
				.window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
				.apply((longFloatTuple2, longFloatTuple22) -> new Tuple2<Long, Float>(longFloatTuple2.f0, longFloatTuple2.f1 + longFloatTuple22.f1));

		recommendPosts.print();

		/*
		 * ====================================================
		 * ====================================================
		 * ================ FRAUD DETECTION  ==================
		 * ====================================================
		 * ====================================================
		 * */

		SourceFunction<CommentEvent> sourceFraudComments = new SimulationSourceFunction<CommentEvent>("comment-topic", "dspa_project.schemas.CommentSchema",
				2, 10000, 10000);

		SourceFunction<PostEvent> sourceFraudPosts = new SimulationSourceFunction<PostEvent>("post-topic", "dspa_project.schemas.PostSchema",
				2, 10000, 10000);

		DataStream<Tuple2<CommentEvent, Boolean>> fraudComments = env.addSource(sourceFraudComments, typeInfoComments)
				.map(new MapFunction<CommentEvent, Tuple2<CommentEvent, Boolean> >() {
					@Override
					public Tuple2<CommentEvent, Boolean> map(CommentEvent commentEvent) throws Exception {
						boolean fraud = UnusualActivityDetection.checkLocation(commentEvent.getPersonId(), commentEvent.getPlaceId());
						return new Tuple2<CommentEvent, Boolean>(commentEvent, fraud);
					}
				})
				.filter(new FilterFunction<Tuple2<CommentEvent, Boolean>>() {
					@Override
					public boolean filter(Tuple2<CommentEvent, Boolean> commentEventTuple2) throws Exception {
						return (commentEventTuple2.f1).booleanValue();
					}
				});

		//recommendComments.print();

		// compute tips per hour for each driver
		DataStream<Tuple2<PostEvent, Boolean>> fraudPosts = env.addSource(sourceFraudPosts, typeInfoPosts)
				.map(new MapFunction<PostEvent, Tuple2<PostEvent, Boolean> >() {
					@Override
					public Tuple2<PostEvent, Boolean> map(PostEvent postEvent) throws Exception {
						boolean fraud = UnusualActivityDetection.checkLocation(postEvent.getPersonId(), postEvent.getPlaceId());
						return new Tuple2<PostEvent, Boolean>(postEvent, fraud);
					}
				})
				.filter(new FilterFunction<Tuple2<PostEvent, Boolean>>() {
					@Override
					public boolean filter(Tuple2<PostEvent, Boolean> posttEventTuple2) throws Exception {
						return (posttEventTuple2.f1).booleanValue();
					}
				});

//		env.execute("Flink Streaming Java API Skeleton");
























//		Properties kafkaProps = new Properties();
//		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
//		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
//		kafkaProps.setProperty("auto.offset.reset", "earliest");
//
//		/*
//		* ====================================================
//		* ====================================================
//		* ================ WINDOW HANDLING  ==================
//		* ====================================================
//		* ====================================================
//		* */
//
//
//		DataStream<Tuple2<Long, LikeEvent>> streamLike = env.addSource(
//				new FlinkKafkaConsumer011<>("like-topic", new LikeSchema(), kafkaProps)
//		);
//
//		streamLike
//				.assignTimestampsAndWatermarks(new LikeTimeWatermarkGenerator())
////						AscendingTimestampExtractor<Tuple2<Long, LikeEvent>>() {
////
////					@Override
////					public long extractAscendingTimestamp(Tuple2<Long, LikeEvent> element) {
////						return element.f1.getCreationDate().getTime();
////					}
////				})
//				.keyBy(0)
//				.process(new LikeProcessFunction())	//
//				.print();
//		//System.out.println("COunt: " + LikeProcessFunction.count);
//
//		//streamLike.print();
//
//		DataStream<CommentEvent> streamComment = env.addSource(
//				new FlinkKafkaConsumer011<>("comment-topic", new CommentSchema(), kafkaProps)
//		);
//		//streamComment.print();
//
//		DataStream<PostEvent> streamPost = env.addSource(
//				new FlinkKafkaConsumer011<>("post-topic", new PostSchema(), kafkaProps)
//		);
//		//streamPost.print();
//
//		env.execute("Flink Streaming Java API Skeleton");
//
//
//
//
//		DataStream<Tuple2<Long, LikeEvent>> streamLikeRecommendations = env.addSource(
//				new FlinkKafkaConsumer011<>("like-topic", new LikeSchema(), kafkaProps)
//		);
//
//		streamLikeRecommendations
//				.assignTimestampsAndWatermarks(new LikeTimeWatermarkGenerator())
////						AscendingTimestampExtractor<Tuple2<Long, LikeEvent>>() {
////
////					@Override
////					public long extractAscendingTimestamp(Tuple2<Long, LikeEvent> element) {
////						return element.f1.getCreationDate().getTime();
////					}
////				})
//				.keyBy(0)
//				.process(new LikeProcessFunction())	//
//				.print();
//		//System.out.println("COunt: " + LikeProcessFunction.count);
//
//		//streamLikeRecommendations.print();
//
//		DataStream<CommentEvent> streamCommentRecommendations = env.addSource(
//				new FlinkKafkaConsumer011<>("comment-topic", new CommentSchema(), kafkaProps)
//		);
//		//streamCommentRecommendations.print();
//
//		DataStream<PostEvent> streamPostRecommendations = env.addSource(
//				new FlinkKafkaConsumer011<>("post-topic", new PostSchema(), kafkaProps)
//		);
//		//streamPostRecommendations.print();
//
//
//
//
//		DataStream<Tuple2<Long, LikeEvent>> streamLikeFraud = env.addSource(
//				new FlinkKafkaConsumer011<>("like-topic", new LikeSchema(), kafkaProps)
//		);
//
//		streamLikeFraud
//				.assignTimestampsAndWatermarks(new LikeTimeWatermarkGenerator())
////						AscendingTimestampExtractor<Tuple2<Long, LikeEvent>>() {
////
////					@Override
////					public long extractAscendingTimestamp(Tuple2<Long, LikeEvent> element) {
////						return element.f1.getCreationDate().getTime();
////					}
////				})
//				.keyBy(0)
//				.process(new LikeProcessFunction())	//
//				.print();
//		//System.out.println("COunt: " + LikeProcessFunction.count);
//
//		//streamLikeFraud.print();
//
//		DataStream<CommentEvent> streamCommentFraud = env.addSource(
//				new FlinkKafkaConsumer011<>("comment-topic", new CommentSchema(), kafkaProps)
//		);
//		//streamCommentFraud.print();
//
//		DataStream<PostEvent> streamPostFraud = env.addSource(
//				new FlinkKafkaConsumer011<>("post-topic", new PostSchema(), kafkaProps)
//		);
		//streamPostFraud.print();


		//env.execute("Flink Streaming Java API Skeleton");


//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Define a like stream
//		DataStream<LikeEvent> likeStream = env.addSource();
//
//		FlinkKafkaProducer011<LikeEvent> likesProducer = new FlinkKafkaProducer011<LikeEvent>(
//				"localhost:9092", // broker list
//				"likes-topic", // target topic
//				 new LikeSchema()); // serialization schema
//
//		likeStream.addSink(likesProducer);
//
//		// Define a comment stream
//		DataStream<CommentEvent> commentStream = env.addSource(le);
//
//		FlinkKafkaProducer011<CommentEvent> commentsProducer = new FlinkKafkaProducer011<CommentEvent>(
//				"localhost:9092", // broker list
//				"comments-topic", // target topic
//				new CommentSchema()); // serialization schema
//
//		commentStream.addSink(commentsProducer);
//
//		// Define a post stream
//		DataStream<PostEvent> postStream = env.addSource(le);
//
//		FlinkKafkaProducer011<PostEvent> postProducer = new FlinkKafkaProducer011<PostEvent>(
//				"localhost:9092", // broker list
//				"posts-topic", // target topic
//				new PostSchema()); // serialization schema
//
//		postStream.addSink(postProducer);
	}

	private static void parseArguments(String[] args) {
		Map<String, List<String>> params = new HashMap<>();

		List<String> options = null;
		for (int i = 0; i < args.length; i++) {
			final String a = args[i];

			if (a.charAt(0) == '-') {
				if (a.length() < 2) {
					System.err.println("Error at argument " + a);
					System.exit(1);
				}

				options = new ArrayList<String>();
				params.put(a.substring(1), options);
			} else if (options != null) {
				options.add(a);
			} else {
				System.err.println("Illegal parameter usage");
				System.exit(1);
			}
		}


		if (params.get("delete") != null) {
			DataLoader.resetTables();
		}

	}

}
