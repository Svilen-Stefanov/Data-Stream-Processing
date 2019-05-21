package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;
import dspa_project.stream.sinks.WriteOutputFormat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class EventCountStream {

    private final String sourceName;

    private final boolean replies;

    private final Time tumblingSize;
    private final Time activeWindow;

    private final DataStream<CountingResults> stream;

    private static class GetTimestamp extends ProcessAllWindowFunction<PostsCounts, CountingResults, TimeWindow> {

        @Override
        public void process(Context context, Iterable<PostsCounts> iterable, Collector<CountingResults> collector) {
            long time = context.window().getEnd();
            PostsCounts out = new PostsCounts();
            for ( PostsCounts posts : iterable) {
                out.putAll(posts);
            }
            collector.collect(new CountingResults(new Date(time),out));
        }
    }

    private static class CountingAggregate implements AggregateFunction<PostsCounts, PostsCounts, PostsCounts> {

        @Override
        public PostsCounts createAccumulator() {
            return new PostsCounts();
        }

        @Override
        public PostsCounts merge(PostsCounts lhs, PostsCounts rhs) {
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
        public PostsCounts add(PostsCounts el, PostsCounts acc) {
            return merge( acc, el );
        }
        @Override
        public PostsCounts getResult(PostsCounts acc) {
            return acc;
        }
    }

    // Count number of replies/comments for an active post. If replies is true replies are counted, otherwise comments are counted
    public EventCountStream( StreamExecutionEnvironment env, String sourceName, Time tumblingSize, Time activeWindow, boolean replies ){
        this.sourceName = sourceName;
        this.tumblingSize = tumblingSize;
        this.activeWindow = activeWindow;
        this.replies = replies;
        AllEventsStream aes = new AllEventsStream( env, sourceName, tumblingSize, activeWindow );
        DataStream<EventsCollection> all_stream = aes.getStream();
        this.stream = calculateCount( all_stream );
    }

    public DataStream<CountingResults> getStream(){
        return stream;
    }

    public void writeToFile( String filename ){
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH:mm:ss");

        int iend = filename.lastIndexOf(".");
        filename = filename.substring(0 , iend) + "-" + formatter.format(date) + filename.substring(iend);
        DataStream<String> task1_1 = getStream().flatMap(new FlatMapFunction<CountingResults, String>() {
            @Override
            public void flatMap(CountingResults countingResults, Collector<String> collector) {
                String output = countingResults.f0.toString();
                for ( Map.Entry<Long,Integer> count: countingResults.f1.entrySet() ){
                    collector.collect( output + "," + count.getKey() + "," + count.getValue() );
                }
            }
        });
        String csvHeader = "CreationDate, PostId, Count";
        task1_1.writeUsingOutputFormat(new WriteOutputFormat(filename, csvHeader)).setParallelism(1);
    }

    private DataStream<CountingResults> calculateCount( DataStream< EventsCollection > all_stream ) {
        final boolean replies = this.replies;

        DataStream<PostsCounts> stream = all_stream.map(new RichMapFunction<EventsCollection, PostsCounts>() {
            @Override
            public PostsCounts map( EventsCollection post ) {
                PostsCounts pc = new PostsCounts();
                EventsCollection ec = post;
                int i=0;
                for ( EventInterface event : ec ) {
                    if ( event instanceof CommentEvent) {
                        CommentEvent ce = (CommentEvent) event;
                        if ( ( ce.getReplyToCommentId() != -1 && replies ) || ( ce.getReplyToCommentId() == -1 && !replies ) ) {
                            i++;
                        }
                    }
                    System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> "+ event );
                }
                pc.put(ec.get(0).getPostId(),i);
                return pc;
            }
        }).process(new ProcessFunction<PostsCounts, PostsCounts>() {
            @Override
            public void processElement(PostsCounts postsCounts, Context context, Collector<PostsCounts> collector) throws Exception {

                System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> " + new Date( context.timestamp() ) + " "+ postsCounts );
                collector.collect(postsCounts);
            }
        });
        return stream.windowAll( SlidingEventTimeWindows.of( activeWindow, tumblingSize ) ).aggregate( new CountingAggregate(), new GetTimestamp() );
    }
}
