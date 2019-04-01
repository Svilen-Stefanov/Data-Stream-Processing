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

import dspa_project.event.CommentEvent;
import dspa_project.event.EventInterface;
import dspa_project.event.LikeEvent;
import dspa_project.event.PostEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


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
//
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		DataStream<EventInterface> stream = env.addSource([1]);
//
//		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
//				"localhost:9092", // broker list
//				"my-topic", // target topic
//				new SimpleStringSchema()); // serialization schema
//
//		stream.addSink(myProducer);



		// set up the streaming execution environment
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		DataStream<WikipediaEditEvent> edits = env.addSource(new
//				WikipediaEditsSource());
//
//		// Task 3
//		Properties kafkaProps = new Properties();
//		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
//		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
//		//kafkaProps.setProperty("group.id", YOUR_GROUP);
//		// always read the Kafka topic from the start
//		kafkaProps.setProperty("auto.offset.reset", "earliest");
//		FlinkKafkaConsumer011<WikipediaEditEvent> consumerWiki = new FlinkKafkaConsumer011<WikipediaEditEvent>(
//				"consume-topic",
//				new DeserializationSchema<WikipediaEditEvent>() {
//					@Override
//					public WikipediaEditEvent deserialize(byte[] bytes) throws IOException {
//						long timestamp = 0;
//						String channel = "";
//						String title = new String(bytes);
//						String diffUrl = "";
//						String user = "";
//						int byteDiff = 0;
//						String summary = "";
//						boolean isMinor = false;
//						boolean isNew = false;
//						boolean isUnpatrolled = false;
//						boolean isBotEdit = false;
//						boolean isSpecial = false;
//						boolean isTalk = false;
//
//						WikipediaEditEvent ev = new WikipediaEditEvent(
//								timestamp, channel, title, diffUrl, user,  byteDiff, summary, isMinor, isNew, isUnpatrolled, isBotEdit, isSpecial, isTalk);
//						return ev;
//					}
//
//					@Override
//					public boolean isEndOfStream(WikipediaEditEvent wikipediaEditEvent) {
//						return false;
//					}
//
//					@Override
//					public TypeInformation<WikipediaEditEvent> getProducedType() {
//						return TypeExtractor.getForClass(WikipediaEditEvent.class);
//					}
//				},
//				kafkaProps
//		);
//
//		DataStream<WikipediaEditEvent> stream = env.addSource(consumerWiki);

		//stream.print();

		// Task 2 -> all subtasks
//		DataStream<String> stream = edits.map(new MapFunction<WikipediaEditEvent, String>() {
//			@Override
//			public String map(WikipediaEditEvent wiki_event) {
//				return wiki_event.toString();
//			}
//		});
//
//		RoundRobinPartitioner<String> round_robin = new RoundRobinPartitioner<>();
//
//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", "localhost:9092");
//
//		FlinkKafkaPartitioner<String> roundRobin = new FlinkKafkaPartitioner<String>() {
//			@Override
//			public int partition(String s, byte[] bytes, byte[] bytes1, String s2, int[] ints) {
//				System.out.println("Part: " + count % ints.length);
//				System.out.println("Length: " + ints.length);
//				return ints[(count++) % ints.length];
//			}
//		};
//
//
//		//Task 2 -> #3
//		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
//						"my-new-topic", // target topic
//				new SimpleStringSchema(),
//				properties, // serialization schema
//				java.util.Optional.of(roundRobin));
//		stream.addSink(myProducer);

//		//Task 2 -> #1
//		FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
//				"localhost:9092", // broker list
//				"my-topic", // target topic
//				new SimpleStringSchema()); // serialization schema
//		stream.addSink(myProducer);



		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

//		DataStream<Tuple2<String, Integer>> result = (DataStream<Tuple2<String, Integer>>) edits
//		// project the event user and the diff
//				.map(new MapFunction<WikipediaEditEvent, Tuple2<String,
//														Integer>>() {
//					@Override
//					public Tuple2<String, Integer> map(WikipediaEditEvent event) {
//						return new Tuple2<>(
//								event.getUser(), event.getByteDiff());
//					}
//				})
//		// group by user
//				.keyBy(0)
//				//.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//				//.timeWindow(Time.seconds(10))
//		// aggregate changes per user
//				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//					@Override
//					public Tuple2<String, Integer> reduce(
//							Tuple2<String, Integer> e1, Tuple2<String, Integer> e2) {
//						return new Tuple2<>(e1.f0, e1.f1 + e2.f1);
//					}
//				})
//				//
//				.filter(new FilterFunction<Tuple2<String, Integer>>() {
//					@Override
//					public boolean filter(Tuple2<String, Integer> value) throws Exception {
//						return value.f1 > 0;
//					}
//				});

//		result.print();

		// execute program
		//env.execute("Flink Streaming Java API Skeleton");
	}
}