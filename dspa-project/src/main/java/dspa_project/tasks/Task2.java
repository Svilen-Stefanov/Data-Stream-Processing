package dspa_project.tasks;

import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import dspa_project.recommender_system.RecommenderSystem;
import dspa_project.stream.operators.*;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

import java.util.HashMap;
import java.util.Map;

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

        // used to initialize static similarity table
        RecommenderSystem recommenderSystem = new RecommenderSystem();

        DataStream<LikeEvent> initRecommendLikes = env.addSource(sourceRecommendationsLikes, typeInfoLikes);
        DataStream<CommentEvent> initRecommendComments = env.addSource(sourceRecommendationsComments, typeInfoComments);
        DataStream<PostEvent> initRecommendPosts = env.addSource(sourceRecommendationsPosts, typeInfoPosts);

        DataStream<Tuple2<Long, Float[]>> recommendLikes = createRecommendLikesStream(initRecommendLikes);
        DataStream<Tuple2<Long, Float[]>> recommendComments = createRecommendCommentsStream(initRecommendComments);
        DataStream<Tuple2<Long, Float[]>> recommendPosts = createRecommendPostsStream(initRecommendPosts);

        recommendLikes.union(recommendComments)
                .union(recommendPosts)
                .keyBy((KeySelector<Tuple2<Long, Float[]>, Long>) longFloatTuple -> longFloatTuple.f0)
                .window( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new GroupEventsByIdAggregateFunction())
                .flatMap(new HashMapToTupleFlatMapFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new SimilarityAggregateFunction ());
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
                .keyBy((KeySelector<LikeEvent, Long>) EventInterface::getPersonId)
                .window( TumblingEventTimeWindows.of( Time.hours( 1 ) ) )
                .aggregate(new RecommendLikeTumblingAggregateFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new RecommendEventAggregateAllFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }

    private DataStream<Tuple2<Long, Float[]>> createRecommendCommentsStream(DataStream<CommentEvent> recommendComments){

        return recommendComments
                .keyBy((KeySelector<CommentEvent, Long>) EventInterface::getPersonId)
                .window( TumblingEventTimeWindows.of( Time.hours( 1 ) ) )
                .aggregate(new RecommendCommentTumblingAggregateFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new RecommendEventAggregateAllFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }

    private DataStream<Tuple2<Long, Float[]>> createRecommendPostsStream(DataStream<PostEvent> recommendPosts){

        return recommendPosts
                .keyBy((KeySelector<PostEvent, Long>) EventInterface::getPersonId)
                .window( TumblingEventTimeWindows.of( Time.hours( 1 ) ) )
                .aggregate(new RecommendPostTumblingAggregateFunction())
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 4 ), Time.hours( 1 ) ) )
                .aggregate(new RecommendEventAggregateAllFunction())
                .flatMap(new HashMapToTupleFlatMapFunction());
    }
}
