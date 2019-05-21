package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public class ReplyAddPostId extends KeyedBroadcastProcessFunction<Long, CommentEvent, PostsCollection, CommentEvent> {
    private final MapStateDescriptor<Long, PostsCollection> postsDescriptor;
    private final long windowCount;
    private final long windowSize;

    ReplyAddPostId(MapStateDescriptor<Long, PostsCollection> postsDescriptor, long windowCount, Time windowSize ){
        this.postsDescriptor = postsDescriptor;
        this.windowCount = windowCount;
        this.windowSize = windowSize.toMilliseconds();
    }

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

        System.out.println("new posts_set: " + new Date(ts) + " size:" + collection.size() + " collection:" + posts);

        // Update windows
        if ( collection.size() == this.windowCount + 1 ) {
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
        bcast_state.put(ts, posts);

        //System.out.println( task_id + "> Size:" + collection.size() );

        //Update replies at the end of a broadcastWindow
        ctx.applyToKeyedState(earlyReplies, new KeyedStateFunction<Long, ValueState<CommentEvent>>() {
            @Override
            public void process(Long timestamp, ValueState<CommentEvent> state) throws Exception {
                if ( state.value() == null ) {
                    return;
                }
                CommentEvent reply = state.value();
                if (reply.getId() == 46277960) {
                    System.out.println(new Date(ts) + " Here:" + posts );
                }

                if ( reply.getCreationDate().getTime() > ts ) {
                    return;
                }

                if (reply.getId() == 46277960) {
                    System.out.println("Here2:" + posts );
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
                if (reply.getId() == 46277960) {
                    System.out.println("Here3:" + posts );
                }
                // Save early replies from being dropped
                for ( Map.Entry<Long,PostsCollection> window: collection ) {
                    for (Map.Entry<Long, CommentsCollection> post : window.getValue().entrySet()) {
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
                }

                if (reply.getId() == 46277960) {
                    System.out.println(task_id + "> Dropped: " + new Date(ts) + " " + reply);
                }
                state.update(null);
            }
        });
    }
    @Override
    // Output (queryId, taxiId, euclidean distance) for every query, if the taxi ride is now ending.
    public void processElement(CommentEvent reply, ReadOnlyContext ctx, Collector<CommentEvent> out) throws Exception {
        //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Process " + new Date(ctx.currentWatermark()) + " " + reply);
        final ValueState<CommentEvent> state = getRuntimeContext().getState(earlyReplies);
        Iterable<Map.Entry<Long, PostsCollection>> posts_broadcast = ctx.getBroadcastState(postsDescriptor).immutableEntries();
        if (reply.getId() == 46277960) {
            int size = -1;
            if (posts_broadcast instanceof Collection) {
                 size = ((Collection<?>) posts_broadcast).size();
            }
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Here: " + size);
        }
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
        long scheduled = ctx.timestamp() + windowSize;
        scheduled -=  scheduled % windowSize;
        scheduled -= 1;
        ctx.timerService().registerEventTimeTimer( scheduled );
        state.update( reply );
        if (reply.getId() == 46277960) {
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Early: " + new Date(ctx.currentWatermark()) + " " + reply);
        }
    }

    public boolean containsParent( CommentEvent reply, CommentsCollection collection ){
        long id = reply.getReplyToCommentId();
        for ( CommentEvent parent : collection ) {
            if (reply.getId() == 46277960) {
                System.out.println("parent: " + parent.getId());
            }
            if ( parent.getId() == id ) {
                /*if ( parent.getReplyToCommentId() != -1 )
                    System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">SavedByReply: " + reply);*/
                return true;
            }
        }
        return false;
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<CommentEvent> out) throws Exception {
        final long task_id = getRuntimeContext().getIndexOfThisSubtask();
        final ValueState<CommentEvent> state = getRuntimeContext().getState(earlyReplies);
        // only clean up state if this timer is the latest timer for this key
        if ( state.value() == null ) {
            return;
        }
        CommentEvent reply = state.value();
        if (reply.getId() == 46277960) {
            System.out.println(new Date(ts) + " Here" );
        }

        if ( reply.getCreationDate().getTime() > ts ) {
            return;
        }

        final ReadOnlyBroadcastState<Long, PostsCollection> bcast_state = ctx.getBroadcastState(postsDescriptor);
        Collection<Map.Entry<Long,PostsCollection>> collection = (Collection<Map.Entry<Long,PostsCollection>>) bcast_state.immutableEntries();
        long id = reply.getReplyToPostId();
        for ( Map.Entry<Long,PostsCollection> window: collection ) {
            PostsCollection posts = window.getValue();
            for (Map.Entry<Long, CommentsCollection> post : posts.entrySet()) {
                if (!containsParent(reply, post.getValue())) {
                    continue;
                }
                id = post.getKey();
                final CommentEvent modified = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                        reply.getContent(), id, reply.getReplyToCommentId(),
                        reply.getPlaceId());
                out.collect(modified);

                return;
            }
        }

        if (reply.getId() == 46277960) {
            System.out.println(task_id + "> Dropped: " + new Date(ts) + " " + reply);
        }
        //state.update(null);
    }
}
