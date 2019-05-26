package dspa_project;

import dspa_project.config.ConfigLoader;
import dspa_project.config.DataLoader;
import dspa_project.database.queries.SQLQuery;
import dspa_project.stream.sources.KafkaCreator;
import dspa_project.tasks.task1.*;

import dspa_project.tasks.task2.Task2_Dynamic;
import dspa_project.tasks.task2.Task2_Static;
import dspa_project.tasks.task3.Task3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.*;

public class Main {

	private static KafkaCreator kafkaCreator = null;
	private static String configFilePath = "config.xml";
	private final static boolean[][] ALL_TASKS = new boolean[][] {
			{ true, true, true }, //Task1.1, Task1.2, Task1.3
			{ true, true }, // Task2_Static, Task2_Dynamic
			{ true } }; //Task3

	private final static boolean[][] No_TASKS = new boolean[][] {
			{ false, false, false }, //Task1.1, Task1.2, Task1.3
			{ false, false }, // Task2_Static, Task2_Dynamic
			{ false } }; //Task3


	public static void main(String[] args) throws Exception {

		boolean[][] tasks = parseArguments(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		if ( tasks[0][0] ) {
			System.out.println("Executing Task 1.1");
			EventCountStream task1_1 = new EventCountStream(env, "Task1_1", Time.minutes(30), Time.hours(12), false);
			task1_1.writeToFile(ConfigLoader.getTask1_1_path());
		}
		if (tasks[0][1]) {
			System.out.println("Executing Task 1.2");
			EventCountStream task1_2 = new EventCountStream(env, "Task1_2", Time.minutes(30), Time.hours(12), true);
			task1_2.writeToFile(ConfigLoader.getTask1_2_path());
		}
		if ( tasks[0][2] ) {
			System.out.println("Executing Task 1.3");
			UniquePeopleCountStream task1_3 = new UniquePeopleCountStream(env, "Task1_3", Time.hours(1), Time.hours(12), false);
			task1_3.writeToFile(ConfigLoader.getTask1_3_path());
		}
		if ( tasks[1][0] ) {
			System.out.println("Executing Task 2 static");
			Task2_Static task2_static = new Task2_Static(env);
		}
		if ( tasks[1][1] ) {
			System.out.println("Executing Task 2 dynamic");
			Task2_Dynamic task2_dynamic = new Task2_Dynamic(env);
		}
		if( tasks[2][0] ) {
			System.out.println("Executing Task 3");
			Task3 task3 = new Task3(env);
		}
		env.execute("Flink Streaming Java API Skeleton");
	}

	private static boolean[][] parseArguments(String[] args) throws IOException, ClassNotFoundException {
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

		if (params.get("config") != null){
			configFilePath = params.get("config").get(0) ;
		}

		System.out.println("Loading " + configFilePath);
		DataLoader dataLoader = new DataLoader(configFilePath);
		dataLoader.parseStaticData();

		if (params.get("delete") != null) {
			SQLQuery.resetTables();
		}

		if ( kafkaCreator == null )
			kafkaCreator = new KafkaCreator(dataLoader);

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

		if (params.get("task") != null){
			boolean[][] tasks = No_TASKS;
			for ( String task : params.get("task") ) {
				switch(task)
				{
					case "1.1":
						tasks[0][0] = true;
						break;
					case "1.2":
						tasks[0][1] = true;
						break;
					case "1.3":
						tasks[0][2] = true;
						break;
					case "2.S":
						tasks[1][0] = true;
						break;
					case "2.D":
						tasks[1][1] = true;
						break;
					case "3":
						tasks[2][0] = true;
						break;
					default:
						System.err.println("Wrong task. Must be one of \"1.1\",  \"1.1\", \"1.3\", \"2.S\", \"2.D\", \"3\".");
						System.exit(1);
				}
			}
			return tasks;
		}
		return ALL_TASKS;
	}
}
