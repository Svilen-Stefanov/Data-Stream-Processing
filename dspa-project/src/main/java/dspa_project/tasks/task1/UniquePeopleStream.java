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

import java.util.*;

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
                    HashMap<Long, HashSet<String>> ppl = lhs.get( key );
                    for ( Map.Entry<Long,HashSet<String>> person : rhs.get( key ).entrySet() ) {
                        if ( ppl.containsKey( person.getKey() ) )  {
                            ppl.get( person.getKey() ).addAll( person.getValue() );
                        } else {
                            ppl.put( person.getKey(), person.getValue() );
                        }
                    }
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
                HashMap<Long, HashSet<String>> ppl = acc.get( el.f0 );
                for ( Map.Entry<Long,HashSet<String>> person : el.f1.entrySet() ) {
                    if ( ppl.containsKey( person.getKey() ) )  {
                        ppl.get( person.getKey() ).addAll( person.getValue() );
                    } else {
                        ppl.put( person.getKey(), person.getValue() );
                    }
                }
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
        DataStream<Tuple2<Date,UniquePeoplePostCollection>> stream = all_stream.map(new MapFunction< EventsCollection, UniquePeople >() {
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
        }).windowAll( SlidingEventTimeWindows.of( this.activeWindow, this.tumblingSize ) ).aggregate( new UniquePeopleAggregate(), new GetTimestampUniquePeople() );
        return stream;
    }


}
