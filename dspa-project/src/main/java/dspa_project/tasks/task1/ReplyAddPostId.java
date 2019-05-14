package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Map;

public class ReplyAddPostId extends KeyedBroadcastProcessFunction<Long, CommentEvent, PostsCollection, CommentEvent> {

    final private static int WINDOW_COUNT = 24;

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
        final BroadcastState<Long, PostsCollection> bcast_state = ctx.getBroadcastState(Task1_2.postsDescriptor);
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
        Iterable<Map.Entry<Long, PostsCollection>> posts_broadcast = ctx.getBroadcastState(Task1_2.postsDescriptor).immutableEntries();
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
