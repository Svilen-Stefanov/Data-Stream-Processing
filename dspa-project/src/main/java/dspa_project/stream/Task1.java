package dspa_project.stream;

import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.HashMap;

public class Task1 {
    final StreamExecutionEnvironment env;

    private class CreateHashMap implements AggregateFunction<CommentEvent, HashMap<Long, ArrayList<CommentEvent>>,  HashMap<Long, ArrayList<CommentEvent>>> {
        @Override
        public HashMap<Long, ArrayList<CommentEvent>> createAccumulator() {
            return new HashMap<>();
        }
        @Override
        public HashMap<Long, ArrayList<CommentEvent>> merge(HashMap<Long, ArrayList<CommentEvent>> lhs, HashMap<Long, ArrayList<CommentEvent>> rhs) {
            lhs.putAll( rhs );
            for ( Long key : rhs.keySet() ) {
                if ( lhs.containsKey(key) ) {
                    lhs.get(key).addAll(rhs.get(key));
                } else {
                    lhs.put( key, rhs.get(key) );
                }
            }
            return lhs;
        }
        @Override
        public HashMap<Long, ArrayList<CommentEvent>> add(CommentEvent value, HashMap<Long, ArrayList<CommentEvent>> acc) {
            if ( !acc.containsKey(value.getReplyToPostId()) ) {
                acc.put(value.getReplyToPostId(), new ArrayList<>());
            }
            acc.get(value.getReplyToPostId()).add(value);
            return acc;
        }
        @Override
        public HashMap<Long, ArrayList<CommentEvent>> getResult(HashMap<Long, ArrayList<CommentEvent>> acc) {
            return acc;
        }
    }

    private class NumberOfComments implements AggregateFunction<HashMap<Long, ArrayList<CommentEvent>>, HashMap<Long, Integer>,  HashMap<Long, Integer>> {
        @Override
        public HashMap<Long, Integer> createAccumulator() {
            return new HashMap<>();
        }
        @Override
        public HashMap<Long, Integer> merge(HashMap<Long, Integer> lhs, HashMap<Long, Integer> rhs) {
            for ( Long key : rhs.keySet() ) {
                if ( lhs.containsKey(key) ) {
                    lhs.put( key, lhs.get(key) + rhs.get(key) );
                } else {
                    lhs.put( key, rhs.get(key) );
                }
            }
            return lhs;
        }
        @Override
        public HashMap<Long, Integer> add(HashMap<Long, ArrayList<CommentEvent>> post_comments, HashMap<Long, Integer> acc) {
            for ( Long postID : post_comments.keySet() ){
                post_comments.get( postID ).size();

                if ( !acc.containsKey(postID) ) {
                    acc.put(postID, 0);
                }
                acc.put(postID, acc.get(postID) + 1);
            }
            return acc;
        }
        @Override
        public HashMap<Long, Integer> getResult(HashMap<Long, Integer> acc) {
            return acc;
        }
    }

    public Task1( StreamExecutionEnvironment env ) {
        this.env = env;
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SourceFunction<CommentEvent> source = new SimulationSourceFunction<>("comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);

        TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);
        DataStream<CommentEvent> comments = env.addSource(source, typeInfoComments);

        DataStream< HashMap<Long, Integer> > task1 = comments.filter(new FilterFunction<CommentEvent>() {
            @Override
            public boolean filter(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId() != -1;
            }
        }).keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId();
            }
        }).window( TumblingEventTimeWindows.of( Time.minutes( 30 ) ) ).aggregate( new CreateHashMap() )
                .windowAll( SlidingEventTimeWindows.of( Time.hours( 12 ), Time.minutes( 30 ) ) ).aggregate( new NumberOfComments() );
        task1.print();
    }
    public void run() throws Exception {
        env.execute("Flink Streaming Java API Skeleton");
    }
}
