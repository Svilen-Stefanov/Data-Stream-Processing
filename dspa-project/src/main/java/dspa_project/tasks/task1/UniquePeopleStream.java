package dspa_project.tasks.task1;

import dspa_project.model.EventInterface;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class UniquePeopleStream {

    private final String sourceName;

    private final DataStream<Tuple2<Date,UniquePeoplePostCollection>> stream;
    private final Time tumblingSize;
    private final Time activeWindow;

    private static class UniquePeopleAggregate extends RichAllWindowFunction<UniquePeople, Tuple2<Date,UniquePeoplePostCollection>, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<UniquePeople> iterable, Collector< Tuple2<Date,UniquePeoplePostCollection>> collector) throws Exception {
            //System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ">Process2 " + new Date(timeWindow.getEnd()) + iterable);
            long time = timeWindow.getEnd();
            UniquePeoplePostCollection out = new UniquePeoplePostCollection();
            for ( UniquePeople post : iterable) {
                if ( !out.containsKey( post.f0 ) ){
                    out.put( post.f0, post.f1 );
                } else {
                    HashMap<Long, HashSet<String>> ppl = out.get( post.f0 );
                    for ( Map.Entry<Long,HashSet<String>> person : post.f1.entrySet() ) {
                        if ( ppl.containsKey( person.getKey() ) )  {
                            ppl.get( person.getKey() ).addAll( person.getValue() );
                        } else {
                            ppl.put( person.getKey(), person.getValue() );
                        }
                    }
                }
            }
            collector.collect(new Tuple2<>(new Date(time),out));
        }
    }

    public UniquePeopleStream( StreamExecutionEnvironment env, String sourceName, Time tumblingSize, Time activeWindow, boolean includePosts ){
        this.sourceName = sourceName;
        this.tumblingSize = tumblingSize;
        this.activeWindow = activeWindow;
        AllEventsStream aes = new AllEventsStream( env, sourceName, tumblingSize, activeWindow, includePosts );
        DataStream<EventsCollection> all_stream = aes.getStream();
        this.stream = createStream( all_stream );
    }

    public DataStream<Tuple2<Date,UniquePeoplePostCollection>> getStream(){
        return stream;
    }

    private DataStream<Tuple2<Date,UniquePeoplePostCollection>> createStream( DataStream< EventsCollection > all_stream ){
        DataStream< Tuple2<Date,UniquePeoplePostCollection> > stream = all_stream.map(new MapFunction< EventsCollection, UniquePeople >() {
            @Override
            public UniquePeople map(EventsCollection in) {
                HashMap<Long, HashSet<String>> ids =  new HashMap<>();
                for ( EventInterface event : in ) {
                    Long id = event.getPersonId();
                    if ( !ids.containsKey(id) ) {
                        ids.put(id, new HashSet<>());
                    }
                    ids.get(id).add( event.getClass().getSimpleName() );
                }
                return new UniquePeople( in.get(0).getPostId(), ids );
            }
        }).windowAll( SlidingEventTimeWindows.of( this.activeWindow, this.tumblingSize ) ).apply(new UniquePeopleAggregate());

        return stream;
    }


}
