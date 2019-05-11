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
import dspa_project.tasks.Task1;
import dspa_project.tasks.Task2;
import dspa_project.tasks.Task3;
import dspa_project.unusual_activity_detection.UnusualActivityDetection;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

		UnusualActivityDetection uad = new UnusualActivityDetection();
		boolean checkCorrect = uad.checkLocation(122, 28);
		System.out.println(checkCorrect);
		checkCorrect = uad.checkLocation(919, 30);
		System.out.println(checkCorrect);
		//System.exit(1);

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

		int i = 0;
		/*Producer<String, LikeEvent> likeProducer = new KafkaProducer<>(props);
		LikeEvent likeEvent = dl.parseLike();
		while (likeEvent != null) {
			if(i%10000 == 0)
				System.out.println("Like: " + i);
			i++;
			likeProducer.send(new ProducerRecord<String, LikeEvent>("like-topic", likeEvent));
			likeEvent = dl.parseLike();
		}

		likeProducer.close();*/

		/*props.put("value.serializer", Class.forName("dspa_project.schemas.CommentSchema"));
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

		commentProducer.close();*/

		/*props.put("value.serializer", Class.forName("dspa_project.schemas.PostSchema"));
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

		postProducer.close();*/

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);

		Task1 task = new Task1(env);
		/*
		 * ====================================================
		 * ====================================================
		 * ================ RECOMMENDATIONS ===================
		 * ====================================================
		 * ====================================================
		 * */

		Task2 task2 = new Task2(env);

		/*
		 * ====================================================
		 * ====================================================
		 * ================ FRAUD DETECTION  ==================
		 * ====================================================
		 * ====================================================
		 * */

		Task3 task3 = new Task3(env);

		//TODO: save all streams to files in all tasks

		env.execute("Flink Streaming Java API Skeleton");
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

/*
 * TODO
 * - RecommendCommentTumblingAggregateFunction: compute dynamic similarity in add() method
 * - Fraud detection: maybe implement a second version of that? --> agree on what we will implement 2 versions of
 * - save output to files
 * - add ratio to the likes/comments/posts similarity (when you calculate the similarity, multiply similarity by a given number)
 * */
