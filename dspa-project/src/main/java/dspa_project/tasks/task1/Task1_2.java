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
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Task1_2 {

    final static MapStateDescriptor<Long, PostsCollection> postsDescriptor = new MapStateDescriptor<>(
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

    private class CreateHashMapAll implements AggregateFunction<EventInterface, PostsCollectionGeneric,  PostsCollectionGeneric> {
        @Override
        public PostsCollectionGeneric createAccumulator() {
            return new PostsCollectionGeneric();
        }
        @Override
        public PostsCollectionGeneric merge(PostsCollectionGeneric lhs, PostsCollectionGeneric rhs) {
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
        public PostsCollectionGeneric add(EventInterface value, PostsCollectionGeneric acc) {
            if ( !acc.containsKey(value.getPostId()) ) {
                acc.put(value.getPostId(), new EventsCollection());
            }
            acc.get(value.getPostId()).add(value);
            return acc;
        }
        @Override
        public PostsCollectionGeneric getResult(PostsCollectionGeneric acc) {
            return acc;
        }
    }

    private class mergeHours implements AggregateFunction<PostsCollection, PostsCollection,  PostsCollection> {
        @Override
        public PostsCollection createAccumulator() {
            return new PostsCollection();
        }

        @Override
        public PostsCollection add(PostsCollection el, PostsCollection acc) {
            return merge( acc, el );
        }

        @Override
        public PostsCollection getResult(PostsCollection acc) {
            return acc;
        }

        @Override
        public PostsCollection merge(PostsCollection lhs, PostsCollection rhs) {
            for ( Long key : rhs.keySet() ) {
                if ( lhs.containsKey(key) ) {
                    CommentsCollection l = lhs.get(key), r = lhs.get(key);
                    l.addAll(r);
                    lhs.put( key, l );
                } else {
                    lhs.put( key, rhs.get(key) );
                }
            }
            return lhs;
        }
    }

    public DataStream<CommentEvent> generateRepliesStream( DataStream<CommentEvent> comments_stream, DataStream<CommentEvent> all_comments ){
        BroadcastStream<PostsCollection> comments_stream_bcast = comments_stream.keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId();
            }
        }).windowAll( TumblingEventTimeWindows.of( Time.minutes( 30 ) ) ).aggregate( new CreateHashMapComments() ).broadcast(postsDescriptor);


        DataStream<CommentEvent> replies_stream = all_comments.filter(new FilterFunction<CommentEvent>() {
            @Override
            public boolean filter(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId() == -1;
            }
        }).keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) throws Exception {
                return ce.getId();
            }
        }).connect(comments_stream_bcast).process(new ReplyAddPostId());

        return replies_stream;
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
            public boolean filter(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId() != -1;
            }
        });

        // Reply's PostID generation
        DataStream<CommentEvent> replies_stream = generateRepliesStream( comments_stream, all_comments );

        // Convert all streams to single type
        DataStream<EventInterface> likes_stream_casted = likes_stream.map(new MapFunction<LikeEvent, EventInterface>() {
            @Override
            public EventInterface map(LikeEvent likeEvent) throws Exception {
                return likeEvent;
            }
        });

        DataStream<EventInterface> replies_stream_casted = replies_stream.map(new MapFunction<CommentEvent, EventInterface>() {
            @Override
            public EventInterface map(CommentEvent reply) throws Exception {
                return reply;
            }
        });

        DataStream<EventInterface> comments_stream_casted = comments_stream.map(new MapFunction<CommentEvent, EventInterface>() {
            @Override
            public EventInterface map(CommentEvent comment) throws Exception {
                return comment;
            }
        });

        // Put all streams together
        DataStream<PostsCollectionGeneric> all_stream = likes_stream_casted.union(comments_stream_casted).union(replies_stream_casted)
                .keyBy(new KeySelector<EventInterface, Long>() {
                    @Override
                    public Long getKey(EventInterface event) throws Exception {
                        return event.getPostId();
                    }
                }).window( TumblingEventTimeWindows.of( Time.minutes( 30 ) ) ).aggregate( new CreateHashMapAll() );

        /*DataStream<?> comments_stream = comments_stream_tumbl.windowAll( SlidingEventTimeWindows.of( Time.hours( 12 ), Time.minutes( 30 ) ) ).aggregate( new mergeHours() )
                .map(new MapFunction<PostsCollection, NumOfCommentsResults>() {
                    @Override
                    public NumOfCommentsResults map(PostsCollection postsCollection) {
                        NumOfCommentsResults res = new NumOfCommentsResults();
                        for ( Map.Entry<Long,CommentsCollection> entry: postsCollection.entrySet() ){
                            res.put( entry.getKey(), entry.getValue().size() );
                        }
                        return res;
                    }
                });*/
        all_stream.print();


    }
}
