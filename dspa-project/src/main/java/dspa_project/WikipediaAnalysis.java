package dspa_project;

import dspa_project.stream.sources.KafkaCreator;
import dspa_project.tasks.Task1;
import dspa_project.tasks.Task2;
import dspa_project.tasks.Task3;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public class WikipediaAnalysis {

	public static void main(String[] args) throws Exception {
		parseArguments(args);
		DataLoader.parseStaticData();

		KafkaCreator kafkaCreator = new KafkaCreator();
//		kafkaCreator.startLikeStream();
//		kafkaCreator.startCommentStream();
//		kafkaCreator.startPostStream();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Task1 task = new Task1(env);

		Task2 task2 = new Task2(env);

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
