package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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
    private final static class MyKeyIteratoin implements KeyedStateFunction<Long, ValueState<ArrayList<Tuple2< Long, CommentEvent>>>> {
        final long ts;
        final BroadcastState<Long, CommentEvent> bcast_state;
        final CommentEvent reply;
        final Context ctx;
        final Collector<CommentEvent> out;
        final int task_id;
        boolean found;
        MyKeyIteratoin( int task_id, CommentEvent reply, Context ctx, Collector<CommentEvent> out, BroadcastState<Long, CommentEvent> bcast_state ) {
            this.ctx = ctx;
            this.task_id = task_id;
            this.out = out;
            this.ts = ctx.timestamp();
            this.reply = reply;
            this.bcast_state = bcast_state;
            this.found = false;
        }
        @Override
        public void process(Long postid, ValueState<ArrayList<Tuple2<Long, CommentEvent>>> state) throws Exception {
            if ( state.value() == null ) {
            //    System.out.println(task_id + "> MKI Scheduled " + new Date(ctx.currentWatermark()) + ": " + reply);
                bcast_state.put(ts, reply);
            }
            //System.out.println(task_id + "> MKI Start " + new Date(ctx.currentWatermark()) + ": " + reply);
            CommentEvent modified = null;
            for ( Tuple2<Long,CommentEvent> comments : state.value() ) {

                //System.out.println(task_id + "> MKI Checking " + new Date(ctx.currentWatermark()) + ": " + comments.f1 + " " + reply + "\n" + (reply.getReplyToCommentId() == comments.f1.getId()) + " " + (ts >= comments.f0) + " " +  (ctx.currentWatermark() >= comments.f0));
                if ( reply.getReplyToCommentId() == comments.f1.getId() && ts >= comments.f0 && ctx.currentWatermark() >= comments.f0) {
                    modified = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                            reply.getContent(), postid, reply.getReplyToCommentId(),
                            reply.getPlaceId());
                    break;
                }
            }
            if ( modified != null ) {
                out.collect(modified);
                ArrayList< Tuple2< Long, CommentEvent> > state_list = state.value();
                ArrayList< Tuple2< Long, CommentEvent> > list_copy = new ArrayList<>(state_list);
                list_copy.add( new Tuple2<>(ts, modified) );
                state.update( list_copy ); // Prevents concurrent modification exception
                //TODO: Schedule a delete
                found = true;
                //System.out.println(task_id + "> MKI Output " + new Date(ctx.currentWatermark()) + ": " + reply);
            } else {
                //System.out.println(task_id + "> MKI Failed " + new Date(ctx.currentWatermark()) + ": " + reply);
            }
        }

        public boolean isFound(){
            return found;
        }

    }
    @Override
    public void processElement(CommentEvent comment, ReadOnlyContext ctx, Collector<CommentEvent> out) throws Exception {
        //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Process " + new Date(ctx.currentWatermark()) + " " + comment);
        final ValueState<ArrayList<Tuple2<Long, CommentEvent>>> state = getRuntimeContext().getState(window);
        final Iterable<Map.Entry<Long, CommentEvent>> posts_broadcast = ctx.getBroadcastState(postsDescriptor).immutableEntries();

        if (state.value() == null) {
            state.update(new ArrayList<>());
            long end_window = ctx.timestamp() + windowSize;
            end_window -= end_window % windowSize;
            end_window -= 1;
            ctx.timerService().registerEventTimeTimer(end_window + windowSize*windowCount ); // When post is created start polling it for aliveness
        }

        ArrayList< Tuple2< Long, CommentEvent> > state_list = state.value();
        ArrayList< Tuple2< Long, CommentEvent> > list_copy = new ArrayList<>(state_list);
        list_copy.add(new Tuple2<>(ctx.timestamp(), comment));
        state.update( list_copy );

        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }

    private void scheduleAllReplies( CommentEvent comment, OnTimerContext ctx ) throws Exception {
        final ValueState<ArrayList<Tuple2<Long, CommentEvent>>> state = getRuntimeContext().getState(window);
        final Iterable<Map.Entry<Long, CommentEvent>> posts_broadcast = ctx.getBroadcastState(postsDescriptor).immutableEntries();

        ArrayList<Tuple2<Long,CommentEvent>> all_modified = new ArrayList<>();
        all_modified.add(new Tuple2<>(ctx.timestamp(),comment));
        boolean was_modifed = true;
        while ( was_modifed ) // Fix replies of replies
        {
            was_modifed = false;
            ArrayList<Tuple2<Long,CommentEvent>> all_modified_new = new ArrayList<>();
            for (Map.Entry<Long, CommentEvent> ts_reply : posts_broadcast) { // Fix replies of comment
                CommentEvent reply = ts_reply.getValue();
                long ts = ts_reply.getKey();
                //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Checking " + new Date(ctx.currentWatermark()) + " " + reply);
                for ( Tuple2<Long, CommentEvent> modified : all_modified ){
                    if (reply.getReplyToCommentId() == modified.f1.getId() && ts >= modified.f0) {
                        //add it to collection
                        CommentEvent output = new CommentEvent(reply.getId(), reply.getPersonId(), reply.getCreationDate(),
                                reply.getContent(), modified.f1.getPostId(), reply.getReplyToCommentId(), reply.getPlaceId());
                        all_modified_new.add(new Tuple2<>(ts, output));
                        ArrayList< Tuple2< Long, CommentEvent> > state_list = state.value();
                        ArrayList< Tuple2< Long, CommentEvent> > list_copy = new ArrayList<>(state_list);
                        list_copy.add(new Tuple2<>(ts, output));
                        state.update( list_copy ); // Prevents concurrent modification exception
                        //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Saving " + new Date(ctx.currentWatermark()) + " " + modified);
                        //schedule in the future to send it
                        ctx.timerService().registerEventTimeTimer(ts);

                        was_modifed = true;
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

        //System.out.println(task_id + "> onTimer " + new Date(ts) );

        if ( state.value() == null ) {
            return;
        }

        if( (ts+1) % windowSize == 0 ) { // Self clean window
            //System.out.println(task_id + "> onTimerSchedule " + new Date(ts) );
            ctx.timerService().registerEventTimeTimer(ts + windowSize);
        }

        boolean alive = false;
        HashSet<Long> tbd = new HashSet<>();
        for ( Tuple2<Long,CommentEvent> comment: state.value() ) {
            if (ts == comment.f0) {  // Comment that initiated scheduling
                //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> Comment found " + new Date(ts) + " " + comment.f1);
                scheduleAllReplies(comment.f1, ctx);
                if (comment.f1.getReplyToCommentId() != -1) { //reply scheduled by processElement
                    //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> TimerOutput " + new Date(ts) + " " + comment.f1);
                    out.collect(comment.f1);
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
        }
    }

    @Override
    public void processBroadcastElement(CommentEvent reply, Context ctx, Collector<CommentEvent> out) throws Exception {
        final long ts = ctx.timestamp();
        final BroadcastState<Long, CommentEvent> bcast_state = ctx.getBroadcastState(postsDescriptor);
        MyKeyIteratoin mki = new MyKeyIteratoin(getRuntimeContext().getIndexOfThisSubtask(),reply, ctx, out,bcast_state);
        ctx.applyToKeyedState(window,mki);

        if ( !mki.isFound() ){
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> BroadcastScheduled " + new Date(ctx.currentWatermark()) + ": " + reply);
            bcast_state.put(ts, reply);
        }
        // Remove old replies
        Iterator<Map.Entry<Long, CommentEvent>> iter = bcast_state.iterator();
        while(iter.hasNext()){
            Map.Entry<Long, CommentEvent> ts_reply = iter.next();
            long old_reply_ts = ts_reply.getKey();
            if(  old_reply_ts < ctx.currentWatermark() ){
                iter.remove();
                //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> BroadcastCleanup " + new Date(ctx.currentWatermark()) +  ": " + ts_reply.getValue() );
            }
        }
    }
}
