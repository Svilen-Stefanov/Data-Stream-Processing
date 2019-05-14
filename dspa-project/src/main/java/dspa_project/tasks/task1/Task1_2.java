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
            "postWindows",
            BasicTypeInfo.LONG_TYPE_INFO, // Time of window
            TypeInformation.of(PostsCollection.class)); // Posts in window
    final private static int WINDOW_COUNT = 24;

    public static class ReplyAddPostId extends KeyedBroadcastProcessFunction<Long, CommentEvent, PostsCollection, CommentEvent> {

        private final ValueStateDescriptor< CommentEvent > earlyReplies =
                new ValueStateDescriptor<>(
                        "replies",
                        TypeInformation.of(CommentEvent.class));
        @Override
        public void open(Configuration config) {
        }

        @Override
        public void processBroadcastElement(PostsCollection posts, Context ctx, Collector<CommentEvent> out) throws Exception {
            final long ts = ctx.timestamp();
            final long task_id = getRuntimeContext().getIndexOfThisSubtask();
            final BroadcastState<Long, PostsCollection> bcast_state = ctx.getBroadcastState(postsDescriptor);
            Collection<Map.Entry<Long,PostsCollection>> collection = (Collection<Map.Entry<Long,PostsCollection>>) bcast_state.entries();

            //System.out.println("new posts_set: " + new Date(ts) + " size:" + collection.size() + " collection:" + posts);

            // Update windows
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
                // If posts are still active update tree
                for ( Map.Entry<Long,CommentsCollection> post: min_entry.getValue().entrySet() ) {
                    for ( Map.Entry<Long,PostsCollection> window: collection ) {
                        if ( window.getValue().containsKey( post.getKey() ) ){
                            window.getValue().get( post.getKey() ).addAll(post.getValue());
                            //System.out.println( task_id +  "> ActivePostForwarding: " + new Date(ts) + " " + post.getValue() + " to "  + window.getValue().get( post.getKey() ) );
                            break;
                        }
                    }
                }
            }

            //System.out.println( task_id + "> Size:" + collection.size() );

            //Update replies at the end of a broadcastWindow
            ctx.applyToKeyedState(earlyReplies, new KeyedStateFunction<Long, ValueState<CommentEvent>>() {
                @Override
                public void process(Long timestamp, ValueState<CommentEvent> state) throws Exception {
                    if ( state.value() == null ) {
                        return;
                    }
                    CommentEvent reply = state.value();
                    if ( reply.getCreationDate().getTime() > ts ) {
                        return;
                    }

                    // Update current tree with identified replies
                    long id = reply.getReplyToPostId();
                    if( id != -1 ) {
                        if ( !posts.containsKey(id) ) {
                            posts.put(id, new CommentsCollection());
                        }
                        posts.get(reply.getReplyToPostId()).add(reply);
                        state.update(null);
                        return;
                    }

                    // Save early replies from being dropped
                    for (Map.Entry<Long, CommentsCollection> post : posts.entrySet()) {
                        if (!containsParent(reply, post.getValue())) {
                            continue;
                        }
                        id = post.getKey();
                        final CommentEvent modified = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                                reply.getContent(), id, reply.getReplyToCommentId(),
                                reply.getPlaceId());
                        out.collect(modified);

                        if ( !posts.containsKey(id) ) {
                            posts.put(id, new CommentsCollection());
                        }
                        posts.get(id).add(modified);
                        //System.out.println( task_id +  "> Saved: " + new Date(ts) + " " + modified );
                        state.update(null);
                        return;
                    }

                    //System.out.println( task_id + "> Dropped: " + new Date(ts) + " " + reply );
                    state.update(null);
                }
            });

            bcast_state.put(ts, posts);
        }
        @Override
        // Output (queryId, taxiId, euclidean distance) for every query, if the taxi ride is now ending.
        public void processElement(CommentEvent reply, ReadOnlyContext ctx, Collector<CommentEvent> out) throws Exception {
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Process " + new Date(ctx.currentWatermark()) + " " + reply);
            final ValueState<CommentEvent> state = getRuntimeContext().getState(earlyReplies);
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
                    assert( state.value() == null );
                    state.update( modified );
                    return;
                }
            }

            // Not found in the set yet
            assert( state.value() == null );
            state.update( reply );
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Early: " + new Date(ctx.currentWatermark()) + " " + reply);
        }

        public boolean containsParent( CommentEvent reply, CommentsCollection collection ){
            long id = reply.getReplyToCommentId();
            for ( CommentEvent parent : collection ) {
                if ( parent.getId() == id ) {
                    /*if ( parent.getReplyToCommentId() != -1 )
                        System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">SavedByReply: " + reply);*/
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
                return ce.getId();
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
