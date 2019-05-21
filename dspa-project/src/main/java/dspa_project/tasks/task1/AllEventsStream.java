package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

public class AllEventsStream {

    private final String sourceName;

    private final Time tumblingSize;
    private final Time activeWindow;

    private final long tumblingWindowCount;

    private final DataStream<EventsCollection> all_stream;

    private DataStream<Tuple2<Date,UniquePeoplePostCollection>> unique_people_stream;


    private final MapStateDescriptor<Long, PostsCollection> postsDescriptor = new MapStateDescriptor<>(
            "postWindows",
            BasicTypeInfo.LONG_TYPE_INFO, // Time of window
            TypeInformation.of(PostsCollection.class)); // Posts in window

    private static class CreateHashMapComments implements AggregateFunction<CommentEvent, PostsCollection,  PostsCollection> {
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

    private static class CreateHashMapAll implements AggregateFunction<EventInterface, EventsCollection,  EventsCollection> {
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

    public AllEventsStream( StreamExecutionEnvironment env, String sourceName, Time tumblingSize, Time activeWindow, boolean includePosts ) {
        this.sourceName = sourceName;

        this.tumblingSize = tumblingSize;
        this.activeWindow = activeWindow;

        long activeMillis = this.activeWindow.toMilliseconds();
        long tumblingMillis = this.tumblingSize.toMilliseconds();

        if ( activeMillis % tumblingMillis != 0 ){
            throw new IllegalArgumentException("Tumbling windows are not compatible.");
        }

        this.tumblingWindowCount = this.activeWindow.toMilliseconds() / this.tumblingSize.toMilliseconds();

        this.all_stream = createStream( env, includePosts );
    }

    public AllEventsStream( StreamExecutionEnvironment env, String sourceName, Time tumblingSize, Time activeWindow ) {
        this(env, sourceName,tumblingSize,activeWindow, false);
    }

    private DataStream<CommentEvent> generateRepliesStream( DataStream<CommentEvent> comments_stream, DataStream<CommentEvent> all_comments ){
        BroadcastStream<PostsCollection> comments_stream_bcast = comments_stream.keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) {
                return ce.getReplyToPostId();
            }
        }).windowAll( TumblingEventTimeWindows.of( this.tumblingSize ) ).aggregate( new CreateHashMapComments() ).broadcast( postsDescriptor );


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
        }).connect( comments_stream_bcast ).process( new ReplyAddPostId( postsDescriptor, tumblingWindowCount, this.tumblingSize ) );

        return replies_stream;
    }

    private DataStream<EventsCollection> createStream( StreamExecutionEnvironment env, boolean includePosts ) {
        // Likes Stream
        SourceFunction<LikeEvent> likes_source = new SimulationSourceFunction<>(sourceName + "_likes","like-topic", "dspa_project.schemas.LikeSchema",
                2, 10000, 100000);
        TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
        DataStream<LikeEvent> likes_stream = env.addSource(likes_source, typeInfoLikes);

        DataStream<PostEvent> posts_stream = null;
        if ( includePosts ) {
            SourceFunction<PostEvent> posts_source = new SimulationSourceFunction<>(sourceName + "_posts", "post-topic", "dspa_project.schemas.PostSchema",
                    2, 10000, 100000);
            TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);
            posts_stream = env.addSource(posts_source, typeInfoPosts);
        }

        // All Comments Stream ( Comments + Replies )
        SourceFunction<CommentEvent> all_comment_source = new SimulationSourceFunction<>(sourceName + "_comments","comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 100000);
        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        DataStream<CommentEvent> all_comments = env.addSource(all_comment_source, typeInfoComments);


        DataStream<CommentEvent> comments_stream = all_comments.filter(new FilterFunction<CommentEvent>() {
            @Override
            public boolean filter(CommentEvent ce) {
                return ce.getReplyToPostId() != -1;
            }
        });

        // Reply's PostID generation
        DataStream<CommentEvent> replies_stream = generateRepliesStream(comments_stream, all_comments);

        // Convert all streams to single type
        DataStream<EventInterface> likes_stream_casted = likes_stream.map(new MapFunction<LikeEvent, EventInterface>() {
            @Override
            public EventInterface map(LikeEvent likeEvent) {
                return likeEvent;
            }
        });

        DataStream<EventInterface> posts_stream_casted = null;
        if ( includePosts ) {
            posts_stream_casted = posts_stream.map(new MapFunction<PostEvent, EventInterface>() {
                @Override
                public EventInterface map(PostEvent postEvent) {
                    return postEvent;
                }
            });
        }

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
        DataStream<EventInterface> all_casted = likes_stream_casted.union(comments_stream_casted).union(replies_stream_casted);
        if ( includePosts ){
            all_casted = all_casted.union(posts_stream_casted);
        }

        DataStream<EventsCollection> all_stream = all_casted
                .keyBy(new KeySelector<EventInterface, Long>() {
                    @Override
                    public Long getKey(EventInterface event) {
                        return event.getPostId();
                    }
                }).window( TumblingEventTimeWindows.of( this.tumblingSize ) ).aggregate( new CreateHashMapAll() );

        return all_stream;
    }

    public DataStream<EventsCollection> getStream(){
        return all_stream;
    }
}
