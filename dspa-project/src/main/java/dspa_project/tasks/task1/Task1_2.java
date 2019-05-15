package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;
import dspa_project.model.LikeEvent;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Task1_2 {

    private final MapStateDescriptor<Long, PostsCollection> postsDescriptor = new MapStateDescriptor<>(
            "postWindows",
            BasicTypeInfo.LONG_TYPE_INFO, // Time of window
            TypeInformation.of(PostsCollection.class)); // Posts in window

    private class CreateHashMapComments implements AggregateFunction<CommentEvent, PostsCollection,  PostsCollection> {
        @Override
        public PostsCollection createAccumulator() {
            return new PostsCollection();
        }
        @Override
        public PostsCollection merge(PostsCollection lhs, PostsCollection rhs) {
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
        public PostsCollection add(CommentEvent value, PostsCollection acc) {
            if ( !acc.containsKey(value.getReplyToPostId()) ) {
                acc.put(value.getReplyToPostId(), new CommentsCollection());
            }
            acc.get(value.getReplyToPostId()).add(value);
            return acc;
        }
        @Override
        public PostsCollection getResult(PostsCollection acc) {
            return acc;
        }
    }

    private static class GetTimestamp extends ProcessWindowFunction<EventsCollection, Tuple2< Long, EventsCollection >, Long, TimeWindow> {

        @Override
        public void process(Long aLong, Context context, Iterable<EventsCollection> iterable, Collector< Tuple2< Long, EventsCollection > > collector) {
            Long time = context.window().getEnd();
            EventsCollection out = new EventsCollection();
            for ( EventsCollection events : iterable) {
                out.addAll(events);
            }
            collector.collect(new Tuple2<>(time,out));
        }
    }

    private class CreateHashMapAll implements AggregateFunction<EventInterface, EventsCollection,  EventsCollection> {
        @Override
        public EventsCollection createAccumulator() {
            return new EventsCollection();
        }
        @Override
        public EventsCollection merge( EventsCollection lhs, EventsCollection rhs ) {
            lhs.addAll(rhs);
            return lhs;
        }
        @Override
        public EventsCollection add(EventInterface value,EventsCollection acc) {
            acc.add(value);
            return acc;
        }
        @Override
        public EventsCollection getResult( EventsCollection acc) {
            return acc;
        }
    }

    private class CountingAggregate implements AggregateFunction<CountingResults,CountingResults,CountingResults> {

        @Override
        public CountingResults createAccumulator() {
            return new CountingResults( null, new PostsCounts());
        }

        @Override
        public CountingResults merge(CountingResults lhs, CountingResults rhs) {
            for ( Long key : rhs.f1.keySet() ) {
                if ( lhs.f1.containsKey(key) ) {
                    lhs.f1.put( key, lhs.f1.get(key) + rhs.f1.get(key) );
                } else {
                    lhs.f1.put( key, rhs.f1.get(key) );
                }
            }

            if ( lhs.f0 == null ){
                lhs.f0 = rhs.f0;
            }

            return lhs;
        }
        @Override
        public CountingResults add(CountingResults el, CountingResults acc) {
            return merge( acc, el );
        }
        @Override
        public CountingResults getResult(CountingResults acc) {
            return acc;
        }
    }

    private DataStream<CommentEvent> generateRepliesStream( DataStream<CommentEvent> comments_stream, DataStream<CommentEvent> all_comments ){
        BroadcastStream<PostsCollection> comments_stream_bcast = comments_stream.keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) {
                return ce.getReplyToPostId();
            }
        }).windowAll( TumblingEventTimeWindows.of( Time.minutes( 30 ) ) ).aggregate( new CreateHashMapComments() ).broadcast(postsDescriptor);


        DataStream<CommentEvent> replies_stream = all_comments.filter(new FilterFunction<CommentEvent>() {
            @Override
            public boolean filter(CommentEvent ce) {
                return ce.getReplyToPostId() == -1;
            }
        }).keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) {
                return ce.getId();
            }
        }).connect(comments_stream_bcast).process(new ReplyAddPostId(postsDescriptor));

        return replies_stream;
    }

    // Count number of replies/comments for an active post. If replies is true replies are counted, otherwise comments are counted
    private DataStream<CountingResults> calculateCount( boolean replies, DataStream< Tuple2< Long, EventsCollection > > all_stream ) {

        DataStream<CountingResults> stream = all_stream.map(new MapFunction< Tuple2< Long, EventsCollection >, CountingResults>() {
            @Override
            public CountingResults map( Tuple2< Long, EventsCollection > post) {
                PostsCounts pc = new PostsCounts();
                EventsCollection ec = post.f1;
                int i=0;
                for ( EventInterface event : ec ) {
                    if ( event instanceof CommentEvent ) {
                        CommentEvent ce = (CommentEvent) event;
                        if ( ( ce.getReplyToCommentId() != -1 && replies ) || ( ce.getReplyToCommentId() == -1 && !replies ) ) {
                            i++;
                        }
                    }
                }
                pc.put(ec.get(0).getPostId(),i);
                CountingResults cr = new CountingResults( new Date( post.f0 ), pc );
                return cr;
            }
        }).windowAll( SlidingEventTimeWindows.of( Time.hours( 12 ), Time.minutes( 30 ) ) ).aggregate( new CountingAggregate() );

        return stream;
    }

    public Task1_2(StreamExecutionEnvironment env ) {

        // Likes Stream
        SourceFunction<LikeEvent> likes_source = new SimulationSourceFunction<>("like-topic", "dspa_project.schemas.LikeSchema",
                2, 10000, 10000);
        TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
        DataStream<LikeEvent> likes_stream = env.addSource(likes_source, typeInfoLikes);

        // All Comments Stream ( Comments + Replies )
        SourceFunction<CommentEvent> all_comment_source = new SimulationSourceFunction<>("comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);
        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        DataStream<CommentEvent> all_comments = env.addSource(all_comment_source, typeInfoComments);


        DataStream<CommentEvent> comments_stream = all_comments.filter(new FilterFunction<CommentEvent>() {
            @Override
            public boolean filter(CommentEvent ce) {
                return ce.getReplyToPostId() != -1;
            }
        });

        // Reply's PostID generation
        DataStream<CommentEvent> replies_stream = generateRepliesStream( comments_stream, all_comments );

        // Convert all streams to single type
        DataStream<EventInterface> likes_stream_casted = likes_stream.map(new MapFunction<LikeEvent, EventInterface>() {
            @Override
            public EventInterface map(LikeEvent likeEvent) {
                return likeEvent;
            }
        });

        DataStream<EventInterface> replies_stream_casted = replies_stream.map(new MapFunction<CommentEvent, EventInterface>() {
            @Override
            public EventInterface map(CommentEvent reply) {
                return reply;
            }
        });

        DataStream<EventInterface> comments_stream_casted = comments_stream.map(new MapFunction<CommentEvent, EventInterface>() {
            @Override
            public EventInterface map(CommentEvent comment) {
                return comment;
            }
        });

        // Put all streams together
        DataStream< Tuple2< Long, EventsCollection > > all_stream = likes_stream_casted.union(comments_stream_casted).union(replies_stream_casted)
                .keyBy(new KeySelector<EventInterface, Long>() {
                    @Override
                    public Long getKey(EventInterface event) {
                        return event.getPostId();
                    }
                }).window( TumblingEventTimeWindows.of( Time.minutes( 30 ) ) ).aggregate( new CreateHashMapAll(), new GetTimestamp() );

        DataStream<CountingResults> number_of_replies_stream = calculateCount(true, all_stream );
        DataStream<CountingResults> number_of_comments_stream = calculateCount(false, all_stream );

        number_of_replies_stream.print();

    }
}
