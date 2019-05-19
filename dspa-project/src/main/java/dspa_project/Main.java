package dspa_project;

import dspa_project.config.DataLoader;
import dspa_project.database.queries.SQLQuery;
import dspa_project.stream.sources.KafkaCreator;
import dspa_project.tasks.task1.*;

import dspa_project.tasks.task2.Task2;
import dspa_project.tasks.task2.Task2_Dynamic;
import dspa_project.tasks.task3.Task3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

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

//		DataStream<CountingResults> task1_1 = new EventCountStream(env, "Task1_1", Time.minutes(30), Time.hours(12), false).getStream();
//		DataStream<CountingResults> task1_2 = new EventCountStream(env,"Task1_2", Time.minutes(30), Time.hours(12), true).getStream();
//		DataStream<Tuple2<Date, PostsCounts>> task1_3 = new UniquePeopleCountStream(env,"Task1_3", Time.hours(1), Time.hours(12), false).getStream();
//		task1_3.print();

//		Task2 task2 = new Task2(env);
//		DataStream<?> task2 = new Task2_Dynamic(env, "Task_2", Time.hours(1), Time.hours(48), true).getStream();
//		task2.print();

		Task3 task3 = new Task3(env);

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
			SQLQuery.resetTables();
		}

		if (params.get("loadKafkaLikes") != null) {
			long count = -1;
			if ( params.get("loadKafkaLikes").size() == 1 ) {
				Long temp = Long.parseLong( params.get("loadKafkaLikes").get(0) );
				count = temp;
			}
			kafkaCreator.startLikeStream( count );
		}

		if (params.get("loadKafkaComments") != null) {
			long count = -1;
			if ( params.get("loadKafkaComments").size() == 1 ) {
				Long temp = Long.parseLong( params.get("loadKafkaComments").get(0) );
				count = temp;
			}
			kafkaCreator.startCommentStream( count );
		}

		if (params.get("loadKafkaPosts") != null) {
			long count = -1;
			if ( params.get("loadKafkaPosts").size() == 1 ) {
				Long temp = Long.parseLong( params.get("loadKafkaPosts").get(0) );
				count = temp;
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
