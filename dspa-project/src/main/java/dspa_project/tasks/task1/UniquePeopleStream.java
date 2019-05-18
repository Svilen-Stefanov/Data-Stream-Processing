package dspa_project.tasks.task1;

import dspa_project.model.EventInterface;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.HashSet;

public class UniquePeopleStream {

    private final String sourceName;

    private final DataStream<Tuple2<Date,UniquePeoplePostCollection>> stream;
    private final Time tumblingSize;
    private final Time activeWindow;

    private static class UniquePeopleAggregate implements AggregateFunction< UniquePeople, UniquePeoplePostCollection, UniquePeoplePostCollection > {

        @Override
        public UniquePeoplePostCollection createAccumulator() {
            return new UniquePeoplePostCollection();
        }

        @Override
        public UniquePeoplePostCollection merge(UniquePeoplePostCollection lhs, UniquePeoplePostCollection rhs) {
            for ( Long key : rhs.keySet() ) {
                if ( lhs.containsKey(key) ) {
                    HashSet<Long> set = lhs.get( key );
                    set.addAll( rhs.get( key ) );
                } else {
                    lhs.put( key, rhs.get(key) );
                }
            }

            return lhs;
        }

        @Override
        public UniquePeoplePostCollection add( UniquePeople el, UniquePeoplePostCollection acc ) {
            if ( !acc.containsKey( el.f0 ) ){
                acc.put( el.f0, el.f1 );
            } else {
                HashSet<Long> set = acc.get( el.f0 );
                set.addAll( el.f1 );
            }
            return acc;
        }
        @Override
        public UniquePeoplePostCollection getResult(UniquePeoplePostCollection acc) {
            return acc;
        }
    }

    private static class GetTimestampUniquePeople extends ProcessAllWindowFunction<UniquePeoplePostCollection, Tuple2<Date,UniquePeoplePostCollection>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<UniquePeoplePostCollection> iterable, Collector<Tuple2<Date, UniquePeoplePostCollection>> collector) {
            long time = context.window().getEnd();
            UniquePeoplePostCollection out = new UniquePeoplePostCollection();
            for ( UniquePeoplePostCollection posts : iterable) {
                out.putAll(posts);
            }
            collector.collect(new Tuple2<>(new Date(time),out));
        }
    }

    public UniquePeopleStream( StreamExecutionEnvironment env, String sourceName, Time tumblingSize, Time activeWindow ){
        this.sourceName = sourceName;
        this.tumblingSize = tumblingSize;
        this.activeWindow = activeWindow;
        AllEventsStream aes = new AllEventsStream( env, sourceName, tumblingSize, activeWindow );
        DataStream<EventsCollection> all_stream = aes.getStream();
        this.stream = createStream( all_stream );
    }

    public DataStream<Tuple2<Date,UniquePeoplePostCollection>> getStream(){
        return stream;
    }

    private DataStream<Tuple2<Date,UniquePeoplePostCollection>> createStream( DataStream< EventsCollection > all_stream ){
        DataStream<Tuple2<Date,UniquePeoplePostCollection>> stream = all_stream.map(new MapFunction< EventsCollection, UniquePeople >() {
            @Override
            public UniquePeople map(EventsCollection in) {
                HashSet<Long> ids =  new HashSet<>();
                for ( EventInterface event : in ) {
                    ids.add( event.getPersonId() );
                }
                return new UniquePeople( in.get(0).getPostId(), ids );
            }
        }).windowAll( SlidingEventTimeWindows.of( this.activeWindow, this.tumblingSize ) ).aggregate( new UniquePeopleAggregate(), new GetTimestampUniquePeople() );
        return stream;
    }


}
