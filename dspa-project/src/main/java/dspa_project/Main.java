package dspa_project;

import dspa_project.config.DataLoader;
import dspa_project.stream.sources.KafkaCreator;
import dspa_project.tasks.task1.Task1;
import dspa_project.tasks.task2.Task2;
import dspa_project.tasks.task3.Task3;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.*;

public class Main {

	private static KafkaCreator kafkaCreator = null;

	public static void main(String[] args) throws Exception {

		if ( kafkaCreator == null )
			kafkaCreator = new KafkaCreator();

		parseArguments(args);
		DataLoader dataLoader = new DataLoader();
		dataLoader.parseStaticData();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Task1 task1 = new Task1(env);
		//task1.getNumberOfCommentsStream().print();
		//task1.getNumberOfRepliesStream().print();
		task1.getUniquePeopleStream().print();

		//Task2 task2 = new Task2(env);

		//Task3 task3 = new Task3(env);

		//TODO: save all streams to files in all tasks

		env.execute("Flink Streaming Java API Skeleton");
	}

	private static void parseArguments(String[] args) throws IOException, ClassNotFoundException {
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

		if (params.get("loadKafkaLikes") != null) {
			long count = -1;
			if ( params.get("loadKafkaLikes").size() == 1 ) {
				Long temp = Long.parseLong( params.get("loadKafkaLikes").get(0) );
				if ( temp != null ){
					count = temp;
				}
			}
			kafkaCreator.startLikeStream( count );
		}

		if (params.get("loadKafkaComments") != null) {
			long count = -1;
			if ( params.get("loadKafkaComments").size() == 1 ) {
				Long temp = Long.parseLong( params.get("loadKafkaComments").get(0) );
				if ( temp != null ){
					count = temp;
				}
			}
			kafkaCreator.startCommentStream( count );
		}

		if (params.get("loadKafkaPosts") != null) {
			long count = -1;
			if ( params.get("loadKafkaPosts").size() == 1 ) {
				Long temp = Long.parseLong( params.get("loadKafkaPosts").get(0) );
				if ( temp != null ){
					count = temp;
				}
			}
			kafkaCreator.startPostStream( count );
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
