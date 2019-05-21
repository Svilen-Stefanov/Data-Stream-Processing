package dspa_project.tasks.task1;

import dspa_project.stream.sinks.WriteOutputFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class UniquePeopleCountStream {

    private final String sourceName;

    private final DataStream<CountingResults> stream;
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

    private DataStream<CountingResults> createStream( DataStream<Tuple2<Date,UniquePeoplePostCollection>> unique_ppl_stream ){
        DataStream<CountingResults> stream = unique_ppl_stream.map(new MapFunction< Tuple2<Date,UniquePeoplePostCollection>, CountingResults >() {

            @Override
            public CountingResults map(Tuple2<Date,UniquePeoplePostCollection> in) {
                PostsCounts pc = new PostsCounts();
                for ( Map.Entry<Long, HashMap<Long, HashSet<String>>> post: in.f1.entrySet() ) {
                    pc.put(  post.getKey(), post.getValue().keySet().size() );
                }
                return new CountingResults( in.f0, pc );
            }
        });
        return stream;
    }


}
