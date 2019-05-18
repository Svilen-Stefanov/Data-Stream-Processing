package dspa_project.tasks.task1;

import dspa_project.model.EventInterface;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.*;

public class UniquePeopleCountStream {

    private final String sourceName;

    private final DataStream<Tuple2<Date,PostsCounts>> stream;
    private final Time tumblingSize;
    private final Time activeWindow;

    public UniquePeopleCountStream(StreamExecutionEnvironment env, String sourceName, Time tumblingSize, Time activeWindow, boolean includePosts ){
        this.sourceName = sourceName;
        this.tumblingSize = tumblingSize;
        this.activeWindow = activeWindow;
        UniquePeopleStream ups = new UniquePeopleStream( env, sourceName, tumblingSize, activeWindow, includePosts );
        DataStream<Tuple2<Date, UniquePeoplePostCollection>> unique_ppl_stream = ups.getStream();
        this.stream = createStream( unique_ppl_stream );
    }

    public DataStream<Tuple2<Date,PostsCounts>> getStream(){
        return stream;
    }

    private DataStream<Tuple2<Date,PostsCounts>> createStream( DataStream<Tuple2<Date,UniquePeoplePostCollection>> unique_ppl_stream ){
        DataStream<Tuple2<Date,PostsCounts>> stream = unique_ppl_stream.map(new MapFunction< Tuple2<Date,UniquePeoplePostCollection>, Tuple2<Date,PostsCounts> >() {

            @Override
            public Tuple2<Date,PostsCounts> map(Tuple2<Date,UniquePeoplePostCollection> in) {
                PostsCounts pc = new PostsCounts();
                for ( Map.Entry<Long, HashMap<Long, HashSet<String>>> post: in.f1.entrySet() ) {
                    pc.put(  post.getKey(), post.getValue().keySet().size() );
                }
                return new Tuple2<>( in.f0, pc );
            }
        });
        return stream;
    }


}
