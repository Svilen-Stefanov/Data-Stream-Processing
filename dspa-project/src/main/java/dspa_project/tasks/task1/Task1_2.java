package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;

public class Task1_2 {

    final static MapStateDescriptor<Long, PostsCollection> postsDescriptor = new MapStateDescriptor<>(
            "replies",
            BasicTypeInfo.LONG_TYPE_INFO,
            TypeInformation.of(PostsCollection.class));
    final private static int WINDOW_COUNT = 24;

    public static class ReplyAddPostId extends KeyedBroadcastProcessFunction<Long, CommentEvent, PostsCollection, CommentEvent> {

        private final ValueStateDescriptor< CommentsCollection > earlyReplies =
                new ValueStateDescriptor<>(
                        "early_replies",
                        TypeInformation.of(CommentsCollection.class));
        @Override
        public void open(Configuration config) {
        }

        @Override
        public void processBroadcastElement(PostsCollection posts, Context ctx, Collector<CommentEvent> out) throws Exception {
            final long ts = ctx.timestamp();
            BroadcastState<Long, PostsCollection> state = ctx.getBroadcastState(postsDescriptor);
            Collection<Map.Entry<Long,PostsCollection>> collection = (Collection<Map.Entry<Long,PostsCollection>>) state.entries();
            //System.out.println("new posts_set: " + new Date(ts) + " size:" + collection.size() + " collection:" + posts);
            if (collection.size() == WINDOW_COUNT+1) {
                int i = 0;
                Map.Entry<Long,PostsCollection> min_entry = null;
                long min_time = -1;
                for ( Map.Entry<Long,PostsCollection> entry: collection) {
                    if (i == 0 || entry.getKey() <= min_time){
                        min_time = entry.getKey();
                        min_entry = entry;
                    }
                    i++;
                }
                collection.remove(min_entry);
            }
            final long task_id = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println( task_id + "> Size:" + collection.size() );
            ctx.applyToKeyedState(earlyReplies, new KeyedStateFunction<Long, ValueState<CommentsCollection>>() {
                @Override
                public void process(Long timestamp, ValueState<CommentsCollection> state) throws Exception {
                    if ( timestamp > ts ) {
                        return;
                    }
                    if ( state.value() == null ) {
                        return;
                    }
                    CommentsCollection cc = state.value();

                    for ( CommentEvent reply : cc ) {
                        boolean saved = false;
                        for (Map.Entry<Long, CommentsCollection> post : posts.entrySet()) {
                            if (!containsParent(reply, post.getValue())) {
                                continue;
                            }
                            final CommentEvent modified = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                                    reply.getContent(), post.getKey(), reply.getReplyToCommentId(),
                                    reply.getPlaceId());
                            out.collect(modified);
                            System.out.println( task_id +  "> Saved: " + new Date(ts) + " " + modified );
                            saved = true;
                        }
                        if( !saved ) {
                            System.out.println( task_id + "> Dropped: " + new Date(ts) + " " + reply );
                        }
                    }

                    state.update(null);
                }
            });
            state.put(ts, posts);
        }
        @Override
        // Output (queryId, taxiId, euclidean distance) for every query, if the taxi ride is now ending.
        public void processElement(CommentEvent reply, ReadOnlyContext ctx, Collector<CommentEvent> out) throws Exception {
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Process " + new Date(ctx.currentWatermark()) + " " + reply);
            Iterable<Map.Entry<Long, PostsCollection>> posts_broadcast = ctx.getBroadcastState(postsDescriptor).immutableEntries();
            for ( Map.Entry<Long, PostsCollection> posts : posts_broadcast ) {
                for ( Map.Entry<Long, CommentsCollection> post : posts.getValue().entrySet() ) {
                    if ( !containsParent(reply, post.getValue()) ) {
                        continue;
                    }
                    final CommentEvent modified = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                            reply.getContent(), post.getKey(), reply.getReplyToCommentId(),
                            reply.getPlaceId());
                    out.collect(modified);
                    return;
                }
            }

            // Not found in the set yet
            final ValueState<CommentsCollection> state = getRuntimeContext().getState(earlyReplies);
            CommentsCollection cc;
            if ( state.value() == null ) {
                cc = new CommentsCollection();
            } else {
                cc = new CommentsCollection( state.value() );
            }
            cc.add( reply );
            state.update( cc );
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Early: " + new Date(ctx.currentWatermark()) + " " + reply);
        }

        public boolean containsParent( CommentEvent reply, CommentsCollection collection ){
            long id = reply.getReplyToCommentId();
            for ( CommentEvent parent : collection ) {
                if ( parent.getId() == id ) {
                    return true;
                }
            }
            return false;
        }
    }

    private class CreateHashMap implements AggregateFunction<CommentEvent, PostsCollection,  PostsCollection> {
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

    public Task1_2(StreamExecutionEnvironment env ) {
        SourceFunction<CommentEvent> source = new SimulationSourceFunction<>("comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);

        TypeInformation<LikeEvent> typeInfoLikes = TypeInformation.of(LikeEvent.class);
        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);
        DataStream<CommentEvent> comments = env.addSource(source, typeInfoComments);

        DataStream<PostsCollection> comments_stream_tumbl = comments.filter(new FilterFunction<CommentEvent>() {
            @Override
            public boolean filter(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId() != -1;
            }
        }).keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId();
            }
        }).windowAll( TumblingEventTimeWindows.of( Time.minutes( 30 ) ) ).aggregate( new CreateHashMap() );

        BroadcastStream<PostsCollection> posts_broadcast = comments_stream_tumbl.broadcast(postsDescriptor);

        DataStream<CommentEvent> replies_stream = comments.filter(new FilterFunction<CommentEvent>() {
            @Override
            public boolean filter(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId() == -1;
            }
        }).keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) throws Exception {
                return ce.getCreationDate().getTime();
            }
        }).connect(posts_broadcast).process(new ReplyAddPostId());

        DataStream<PostsCollection> replies_stream_tumbl = replies_stream.keyBy(new KeySelector<CommentEvent, Long>() {
            @Override
            public Long getKey(CommentEvent ce) throws Exception {
                return ce.getReplyToPostId();
            }
        }).windowAll( TumblingEventTimeWindows.of( Time.minutes( 30 ) ) ).aggregate( new CreateHashMap() );

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
        replies_stream.print();


    }
}
