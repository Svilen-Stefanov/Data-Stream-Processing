package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.concurrent.java8.FuturesConvertersImpl;

import java.util.*;

public class ReplyAddPostId extends KeyedBroadcastProcessFunction<Long, CommentEvent, CommentEvent, CommentEvent> {
    private final MapStateDescriptor<Long, CommentEvent> postsDescriptor;
    private final long windowSize;
    private final long windowCount;

    ReplyAddPostId(MapStateDescriptor<Long, CommentEvent> postsDescriptor, long windowCount, Time windowSize ){
        this.postsDescriptor = postsDescriptor;
        this.windowSize = windowSize.toMilliseconds();
        this.windowCount = windowCount;
    }

    private final ValueStateDescriptor<ArrayList<Tuple2< Long, CommentEvent>>> window = new ValueStateDescriptor<>("window_comments", TypeInformation.of(new TypeHint
            <ArrayList<Tuple2<Long,CommentEvent>>>(){}));

    private final ValueStateDescriptor<Long> windowStart = new ValueStateDescriptor<>("window_start", BasicTypeInfo.LONG_TYPE_INFO, null);

    @Override
    public void processElement(CommentEvent comment, ReadOnlyContext ctx, Collector<CommentEvent> out) throws Exception {
        //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Process " + new Date(ctx.currentWatermark()) + " " + comment);
        final ValueState<Long> windowSt = getRuntimeContext().getState(windowStart);
        final ValueState<ArrayList<Tuple2<Long, CommentEvent>>> state = getRuntimeContext().getState(window);
        final Iterable<Map.Entry<Long, CommentEvent>> posts_broadcast = ctx.getBroadcastState(postsDescriptor).immutableEntries();

        long end_window = ctx.timestamp() + windowSize;
        end_window -= end_window % windowSize;
        end_window -= 1;

        if (state.value() == null) {
            state.update(new ArrayList<>());
        }
        if (windowSt.value() == null) {
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Scheduled at " + new Date(ctx.currentWatermark()) + " for " + new Date(end_window));
            windowSt.update(end_window);
            ctx.timerService().registerEventTimeTimer(end_window); // When post is created start polling it for aliveness
        }
        if ( windowSt.value() > end_window ){
            ctx.timerService().deleteEventTimeTimer( windowSt.value() );
            windowSt.update(end_window);
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">ReScheduled at " + new Date(ctx.currentWatermark()) + " for " + new Date(end_window));
            ctx.timerService().registerEventTimeTimer(end_window);
        }
        ArrayList< Tuple2< Long, CommentEvent> > state_list = state.value();
        ArrayList< Tuple2< Long, CommentEvent> > list_copy = new ArrayList<>(state_list);
        list_copy.add(new Tuple2<>(ctx.timestamp(), comment));
        state.update( list_copy );
    }

    private void oldReplies(){

    }

    private void recursiveReplies( long watermark, long comment_ts, CommentEvent comment, OnTimerContext ctx, Collector<CommentEvent> out ) throws Exception {
        final ValueState<ArrayList<Tuple2<Long, CommentEvent>>> state = getRuntimeContext().getState(window);
        final Iterable<Map.Entry<Long, CommentEvent>> posts_broadcast = ctx.getBroadcastState(postsDescriptor).immutableEntries();

        ArrayList<Tuple2<Long,CommentEvent>> all_modified = new ArrayList<>();
        all_modified.add(new Tuple2<>(comment_ts,comment));
        boolean was_modifed = true;
        while ( was_modifed ) // Fix replies of replies
        {
            was_modifed = false;
            ArrayList<Tuple2<Long,CommentEvent>> all_modified_new = new ArrayList<>();
            for (Map.Entry<Long, CommentEvent> ts_reply : posts_broadcast) { // Fix replies of comment
                CommentEvent reply = ts_reply.getValue();
                long ts = ts_reply.getKey();
                //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Checking " + new Date(watermark) + " " + reply);
                for ( Tuple2<Long, CommentEvent> modified : all_modified ){
                    if (reply.getReplyToCommentId() == modified.f1.getId() && ts >= modified.f0 && ts <= watermark) {
                        //add it to collection
                        CommentEvent output = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                                reply.getContent(), modified.f1.getPostId(), reply.getReplyToCommentId(), reply.getPlaceId());
                        all_modified_new.add(new Tuple2<>(ts, output));
                        ArrayList< Tuple2< Long, CommentEvent> > state_list = state.value();
                        ArrayList< Tuple2< Long, CommentEvent> > list_copy = new ArrayList<>(state_list);
                        list_copy.add(new Tuple2<>(ts, output));
                        state.update( list_copy ); // Prevents concurrent modification exception
                        out.collect(output);
                        was_modifed = true;

                        //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Output " + new Date(watermark) + " " + output);
                    }
                }
            }
            all_modified = all_modified_new;
        }
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<CommentEvent> out) throws Exception {
        final long task_id = getRuntimeContext().getIndexOfThisSubtask();
        final ValueState<ArrayList<Tuple2<Long, CommentEvent>>> state = getRuntimeContext().getState(window);
        final ValueState<Long> windowSt = getRuntimeContext().getState(windowStart);
        final Iterable<Map.Entry<Long, CommentEvent>> posts_broadcast = ctx.getBroadcastState(postsDescriptor).immutableEntries();

        //System.out.println(task_id + "> onTimer " + new Date(ts) );

        if ( state.value() == null ) {
            return;
        }

        ctx.timerService().registerEventTimeTimer(ts + windowSize);

        ArrayList<Tuple2<Long,CommentEvent>> tocheck = new ArrayList<>();
        for (Map.Entry<Long, CommentEvent> reply : posts_broadcast) {
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> CheckingToCheck " + new Date(ts) + " " + reply);
            if ( ts >= reply.getKey() && ts - windowSize < reply.getKey() ) {
                tocheck.add(new Tuple2<>(reply.getKey(),reply.getValue()));
                //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> ToCheck " + new Date(ts) + " " + reply);
            }
        }

        boolean alive = false;
        HashSet<Long> tbd = new HashSet<>();
        for ( Tuple2<Long,CommentEvent> comment: state.value() ) {
            if (ts >= comment.f0 && ts - windowSize < comment.f0) {  // Comment that initiated scheduling
                //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Comment found " + new Date(ts) + " " + comment.f1);
                recursiveReplies(ts, comment.f0, comment.f1, ctx, out);
            }

            if( ts - windowSize >= comment.f0 )
            {
                for (Tuple2<Long, CommentEvent> ts_reply : tocheck) {
                    CommentEvent reply = ts_reply.f1;
                    //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Checking " + new Date(ts) + " " + comment.f1 + " " + reply);
                    if ( reply.getReplyToCommentId() == comment.f1.getId() ) {
                        alive = true;
                        //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Found " + new Date(ts) + " " + comment.f1 + " " + reply);
                        CommentEvent output = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                                reply.getContent(), comment.f1.getPostId(), reply.getReplyToCommentId(), reply.getPlaceId());
                        ArrayList< Tuple2< Long, CommentEvent> > state_list = state.value();
                        ArrayList< Tuple2< Long, CommentEvent> > list_copy = new ArrayList<>(state_list);
                        list_copy.add(new Tuple2<>(ts_reply.f0, output));
                        state.update( list_copy ); // Prevents concurrent modification exception
                        out.collect(output);
                        recursiveReplies( ts, ts_reply.f0, reply, ctx, out );
                    }
                }
            }

            if ( ts - windowSize*windowCount >= comment.f0 ) {
                tbd.add(comment.f0);
            }
            if ( ts - windowSize*windowCount < comment.f0 && ts >= comment.f0 ) { // Window remains alive
                alive = true;
            }
        }

        if ( alive )
            return;

        //System.out.println(task_id + "> DeleteTime " + new Date(ts) );

        // Inactivity happened - drop old events
        Iterator<Tuple2<Long,CommentEvent>> iter = state.value().iterator();
        while(iter.hasNext()){
            Tuple2<Long, CommentEvent> tuple = iter.next();
            if(tbd.contains( tuple.f0 )){
                iter.remove();
                //System.out.println(task_id + "> Dropping from window at " + new Date(ts) +  ": " + tuple.f1 );
            }
        }

        if ( state.value().size() == 0 ) {
            state.update(null);
            windowSt.update(null);
        }
    }

    @Override
    public void processBroadcastElement(CommentEvent reply, Context ctx, Collector<CommentEvent> out) throws Exception {
        final long ts = ctx.timestamp();
        final BroadcastState<Long, CommentEvent> bcast_state = ctx.getBroadcastState(postsDescriptor);

        //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Broadcast " + new Date(ctx.currentWatermark()) + ", " +  new Date(ts) +  ": " + reply );
        bcast_state.put(ts, reply);

        // Remove old replies
        Iterator<Map.Entry<Long, CommentEvent>> iter = bcast_state.iterator();
        while(iter.hasNext()){
            Map.Entry<Long, CommentEvent> ts_reply = iter.next();
            long old_reply_ts = ts_reply.getKey();
            long end_window = old_reply_ts + windowSize;
            end_window -= end_window % windowSize;
            end_window -= 1;
            if(  end_window < ctx.currentWatermark() ){
                iter.remove();
                //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> BroadcastCleanup " + new Date(ctx.currentWatermark()) +  ": " + ts_reply.getValue() );
            }
        }
    }
}
