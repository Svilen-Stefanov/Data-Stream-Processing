package dspa_project.tasks;

import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import dspa_project.recommender_system.RecommenderSystem;
import dspa_project.stream.operators.RecommendCommentAggregateFunction;
import dspa_project.stream.operators.RecommendLikeAggregateFunction;
import dspa_project.stream.operators.RecommendPostAggregateFunction;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class Task2 {

    public Task2( StreamExecutionEnvironment env ) {
        SourceFunction<LikeEvent> sourceRecommendationsLikes = new SimulationSourceFunction<LikeEvent>("like-topic", "dspa_project.schemas.LikeSchema",
                2, 10000, 10000);

        SourceFunction<CommentEvent> sourceRecommendationsComments = new SimulationSourceFunction<CommentEvent>("comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);

        SourceFunction<PostEvent> sourceRecommendationsPosts = new SimulationSourceFunction<PostEvent>("post-topic", "dspa_project.schemas.PostSchema",
                2, 10000, 10000);

        TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);

        RecommenderSystem recommenderSystem = new RecommenderSystem();

        DataStream<LikeEvent> initRecommendLikes = env.addSource(sourceRecommendationsLikes, typeInfoLikes);
        DataStream<CommentEvent> initRecommendComments = env.addSource(sourceRecommendationsComments, typeInfoComments);
        DataStream<PostEvent> initRecommendPosts = env.addSource(sourceRecommendationsPosts, typeInfoPosts);

        DataStream<Tuple2<Long, Float[]>> recommendLikes = createRecommendLikesStream(initRecommendLikes);
        DataStream<Tuple2<Long, Float[]>> recommendComments = createRecommendCommentsStream(initRecommendComments);
        DataStream<Tuple2<Long, Float[]>> recommendPosts = createRecommendPostsStream(initRecommendPosts);

        recommendLikes.join(recommendComments)
                .where((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple -> longFloatTuple.f0)
                .equalTo((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple -> longFloatTuple.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .apply((longFloatTuple2, longFloatTuple22) -> new Tuple2<>(longFloatTuple2.f0, mergeSumDynamicSimilarity(longFloatTuple2.f1, longFloatTuple22.f1)))
                .join(recommendPosts)
                .where((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple -> longFloatTuple.f0)
                .equalTo((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple -> longFloatTuple.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .apply((longFloatTuple2, longFloatTuple22) -> new Tuple2<>(longFloatTuple2.f0, mergeSumDynamicSimilarity(longFloatTuple2.f1, longFloatTuple22.f1)))
                .keyBy((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple2 -> longFloatTuple2.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .process(new ProcessWindowFunction<Tuple2<Long, Float[]>, Vector<Vector<Tuple2<Long, Float>>>, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<Tuple2<Long, Float[]>> iterable, Collector<Vector<Vector<Tuple2<Long, Float>>>> collector) throws Exception {
                        Vector<Vector<Tuple2<Long, Float>>> similarity = recommenderSystem.getSortedSimilarity(iterable);
                        collector.collect(similarity);
                    }
                });
    }

    private class HashMapToTupleFlatMapFunction implements FlatMapFunction<HashMap<Long, Float[]>, Tuple2<Long, Float[]>>{
        @Override
        public void flatMap(HashMap<Long, Float[]> longFloatHashMap, Collector<Tuple2<Long, Float[]>> collector) throws Exception {
            for (Map.Entry<Long, Float[]> entry : longFloatHashMap.entrySet()) {
                collector.collect(new Tuple2<>(entry.getKey(), entry.getValue()));
            }
        }
    }

    private DataStream<Tuple2<Long, Float[]>> createRecommendLikesStream(DataStream<LikeEvent> recommendLikes){

        return recommendLikes
                .keyBy((KeySelector<LikeEvent, Long>) EventInterface::getPersonId)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .aggregate(new RecommendLikeAggregateFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }

    private DataStream<Tuple2<Long, Float[]>> createRecommendCommentsStream(DataStream<CommentEvent> recommendComments){

        return recommendComments
                .keyBy((KeySelector<CommentEvent, Long>) EventInterface::getPersonId)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .aggregate(new RecommendCommentAggregateFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }

    private DataStream<Tuple2<Long, Float[]>> createRecommendPostsStream(DataStream<PostEvent> recommendPosts){

        return recommendPosts
                .keyBy((KeySelector<PostEvent, Long>) EventInterface::getPersonId)
                .window(SlidingEventTimeWindows.of(Time.hours(4), Time.hours(1)))
                .aggregate(new RecommendPostAggregateFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }

    //TODO: this is actually a heuristic
    private Float[] mergeSumDynamicSimilarity(Float[] f1, Float[] f2){
        Float[] mergedSum = new Float[10];
        for (int i = 0; i < f1.length; i++) {
            mergedSum[i] = f1[i] + f2[i];
        }
        return mergedSum;
    }
}
