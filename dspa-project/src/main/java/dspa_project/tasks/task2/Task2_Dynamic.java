package dspa_project.tasks.task2;

import dspa_project.config.ConfigLoader;
import dspa_project.stream.operators.GroupEventsByIdAggregateFunction;
import dspa_project.stream.operators.SimilarityAggregateFunction;
import dspa_project.stream.sinks.WriteOutputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class Task2_Dynamic {

    public Task2_Dynamic(StreamExecutionEnvironment env ) {

        // used to initialize static similarity table
        RecommenderSystem recommenderSystem = new RecommenderSystem();

        DataStream<Tuple3<Date,Long, Float[]>> dynamic_similarity = new SimilarityStream(env, "Task_2_Dynamic", Time.hours(1), Time.hours(4), true).getStream();
        DataStream<Tuple2<Integer, Vector<Tuple2<Long, Float>>>> finalRecommendation = dynamic_similarity.map(new MapFunction<Tuple3<Date, Long, Float[]>, Tuple2< Long, Float[]>>() {
            @Override
            public Tuple2<Long, Float[]> map(Tuple3<Date, Long, Float[]> in) throws Exception {
                return new Tuple2<>(in.f1, in.f2);
            }
        }).keyBy((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple -> longFloatTuple.f0)
                .window( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new GroupEventsByIdAggregateFunction())
                .flatMap(new HashMapToTupleFlatMapFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new SimilarityAggregateFunction ())
                .flatMap(new FlatMapFunction<Vector<Vector<Tuple2<Long, Float>>>, Tuple2<Integer, Vector<Tuple2<Long, Float>>>>() {
                    @Override
                    public void flatMap(Vector<Vector<Tuple2<Long, Float>>> vectors, Collector<Tuple2<Integer, Vector<Tuple2<Long, Float>>>> collector) throws Exception {
                        for (int i = 0; i < vectors.size(); i++) {
                            collector.collect(new Tuple2<>(i, vectors.get(i)));
                        }
                    }
                });

        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH:mm:ss");

        String fileName = ConfigLoader.getTask2_dynamic_path();
        int iend = fileName.lastIndexOf(".");
        String csvHeader = "Suggestion 1, Suggestion 2, Suggestion 3, Suggestion 4, Suggestion 5";

        for (int i = 0; i < RecommenderSystem.SELECTED_USERS.length; i++) {
            final int curId = i;
            String saveFilePath = fileName.substring(0 , iend) + "-"
                    + "user" + "-" + RecommenderSystem.SELECTED_USERS[i] + "-"
                    + formatter.format(date) + fileName.substring(iend);

            finalRecommendation
                    .filter(new FilterFunction<Tuple2<Integer, Vector<Tuple2<Long, Float>>>>() {
                        @Override
                        public boolean filter(Tuple2<Integer, Vector<Tuple2<Long, Float>>> integerVectorTuple2) {
                            return integerVectorTuple2.f0 == curId;
                        }
                    })
                    .map(new MapFunction<Tuple2<Integer, Vector<Tuple2<Long, Float>>>, String>() {
                        @Override
                        public String map(Tuple2<Integer, Vector<Tuple2<Long, Float>>> integerVectorTuple2) {
                            String output = "";
                            Vector<Tuple2<Long, Float>> vector = integerVectorTuple2.f1;
                            for (int i = 0; i < vector.size(); i++) {
                                if (vector.get(i).f1.equals(0f)){
                                    output += "None,";
                                } else {
                                    output += vector.get(i).f0 + ": " + vector.get(i).f1 + ",";
                                }
                            }
                            output = output.substring(0, output.length() - 1);
                            return output;
                        }
                    })
                    .writeUsingOutputFormat(new WriteOutputFormat(saveFilePath, csvHeader)).setParallelism(1);
        }
    }

    private class HashMapToTupleFlatMapFunction implements FlatMapFunction<HashMap<Long, Tuple2<Float[], Integer>>, Tuple2<Long, Float[]>>{
        @Override
        public void flatMap(HashMap<Long, Tuple2<Float[], Integer>> longFloatHashMap, Collector<Tuple2<Long, Float[]>> collector) {
            for (Map.Entry<Long, Tuple2<Float[], Integer>> entry : longFloatHashMap.entrySet()) {
                Float[] finalSimilarity = new Float[entry.getValue().f0.length];
                for (int i = 0; i < entry.getValue().f0.length; i++) {
                    finalSimilarity[i] = entry.getValue().f0[i] / entry.getValue().f1;
                }
                collector.collect(new Tuple2<>(entry.getKey(), finalSimilarity));
            }
        }
    }
}
