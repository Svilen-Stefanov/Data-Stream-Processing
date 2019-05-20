package dspa_project.tasks.task2;

import dspa_project.config.ConfigLoader;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import dspa_project.stream.operators.*;
import dspa_project.stream.sinks.WriteOutputFormat;
import dspa_project.stream.sources.SimulationSourceFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class Task2_Static {

    public Task2_Static(StreamExecutionEnvironment env ) {
        SourceFunction<LikeEvent> sourceRecommendationsLikes = new SimulationSourceFunction<LikeEvent>("Task2_Static", "like-topic", "dspa_project.schemas.LikeSchema",
                2, 10000, 10000);

        SourceFunction<CommentEvent> sourceRecommendationsComments = new SimulationSourceFunction<CommentEvent>("Task2_Static","comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);

        SourceFunction<PostEvent> sourceRecommendationsPosts = new SimulationSourceFunction<PostEvent>("Task2_Static","post-topic", "dspa_project.schemas.PostSchema",
                2, 10000, 10000);

        TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);

        // used to initialize static similarity table
        RecommenderSystem recommenderSystem = new RecommenderSystem();

        DataStream<LikeEvent> initRecommendLikes = env.addSource(sourceRecommendationsLikes, typeInfoLikes);
        DataStream<CommentEvent> initRecommendComments = env.addSource(sourceRecommendationsComments, typeInfoComments);
        DataStream<PostEvent> initRecommendPosts = env.addSource(sourceRecommendationsPosts, typeInfoPosts);

        DataStream<Tuple2<Long, Float[]>> recommendLikes = createRecommendLikesStream(initRecommendLikes);
        DataStream<Tuple2<Long, Float[]>> recommendComments = createRecommendCommentsStream(initRecommendComments);
        DataStream<Tuple2<Long, Float[]>> recommendPosts = createRecommendPostsStream(initRecommendPosts);

        DataStream<Tuple2<Integer, Vector<Tuple2<Long, Float>>>> finalRecommendation = recommendLikes.union(recommendComments)
                .union(recommendPosts)
                .keyBy((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple -> longFloatTuple.f0)
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

        String fileName = ConfigLoader.getTask2_static_path();
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
                        public boolean filter(Tuple2<Integer, Vector<Tuple2<Long, Float>>> integerVectorTuple2) throws Exception {
                            return integerVectorTuple2.f0 == curId;
                        }
                    })
                    .map(new MapFunction<Tuple2<Integer, Vector<Tuple2<Long, Float>>>, String>() {
                        @Override
                        public String map(Tuple2<Integer, Vector<Tuple2<Long, Float>>> integerVectorTuple2) throws Exception {
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

    private DataStream<Tuple2<Long, Float[]>> createRecommendLikesStream(DataStream<LikeEvent> recommendLikes){

        return recommendLikes
                .keyBy(new KeySelector<LikeEvent, Long>() {
                    @Override
                    public Long getKey(LikeEvent likeEvent) throws Exception {
                        return likeEvent.getPersonId();
                    }
                })
                .window( TumblingEventTimeWindows.of( Time.hours( 1 ) ) )
                .aggregate(new RecommendLikeTumblingAggregateFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new RecommendEventAggregateAllFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }

    private DataStream<Tuple2<Long, Float[]>> createRecommendCommentsStream(DataStream<CommentEvent> recommendComments){

        return recommendComments
                .keyBy(new KeySelector<CommentEvent, Long>() {
                    @Override
                    public Long getKey(CommentEvent commentEvent) throws Exception {
                        return commentEvent.getPersonId();
                    }
                })
                .window( TumblingEventTimeWindows.of( Time.hours( 1 ) ) )
                .aggregate(new RecommendCommentTumblingAggregateFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new RecommendEventAggregateAllFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }

    private DataStream<Tuple2<Long, Float[]>> createRecommendPostsStream(DataStream<PostEvent> recommendPosts){

        return recommendPosts
                .keyBy(new KeySelector<PostEvent, Long>() {
                    @Override
                    public Long getKey(PostEvent postEvent) throws Exception {
                        return postEvent.getPersonId();
                    }
                })
                .window( TumblingEventTimeWindows.of( Time.hours( 1 ) ) )
                .aggregate(new RecommendPostTumblingAggregateFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new RecommendEventAggregateAllFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }
}
