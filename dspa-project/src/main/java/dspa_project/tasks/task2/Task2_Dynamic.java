package dspa_project.tasks.task2;

import dspa_project.tasks.task1.UniquePeople;
import dspa_project.tasks.task1.UniquePeoplePostCollection;
import dspa_project.tasks.task1.UniquePeopleStream;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;

public class Task2_Dynamic {
    private final String sourceName;

    private final DataStream<Tuple3<Date, Long, Float[]>> stream;
    private final Time tumblingSize;
    private final Time activeWindow;

    private final static HashMap<String,Integer> hyperparam_idx = new HashMap<String,Integer>() {{
        put("PostEvent", 0);
        put("LikeEvent", 1);
        put("CommentEvent", 2);
    }};

    private final static float [][] hyperparams = {
            //Post Like  Comment
            {0.0f, 0.6f, 0.4f}, // Post
            {0.6f, 0.2f, 0.1f}, // Like
            {0.4f, 0.1f, 0.2f}, // Comment
    };

    public Task2_Dynamic(StreamExecutionEnvironment env, String sourceName, Time tumblingSize, Time activeWindow, boolean includePosts ){
        this.sourceName = sourceName;
        this.tumblingSize = tumblingSize;
        this.activeWindow = activeWindow;
        UniquePeopleStream ups = new UniquePeopleStream( env, sourceName, tumblingSize, activeWindow, includePosts );
        DataStream<Tuple2<Date, UniquePeoplePostCollection>> unique_ppl_stream = ups.getStream();
        this.stream = createStream( unique_ppl_stream );
    }

    public DataStream<Tuple3<Date, Long, Float[]>> getStream(){
        return stream;
    }

    private DataStream<Tuple3<Date, Long, Float[]>> createStream( DataStream<Tuple2<Date,UniquePeoplePostCollection>> unique_ppl_stream ){
        DataStream<Tuple3<Date, Long, Float[]>> stream = unique_ppl_stream.flatMap(new FlatMapFunction<Tuple2<Date, UniquePeoplePostCollection>, Tuple2<Date, UniquePeople>>() {
            @Override
            public void flatMap(Tuple2<Date, UniquePeoplePostCollection> window, Collector<Tuple2<Date, UniquePeople>> collector) throws Exception {
                for ( Map.Entry<Long, HashMap<Long, HashSet<String>>> post: window.f1.entrySet() ) {
                    collector.collect(new Tuple2<>(window.f0, new UniquePeople(post.getKey(), post.getValue())));
                }
            }
        }).filter(new FilterFunction<Tuple2<Date, UniquePeople>>() {
            @Override
            public boolean filter(Tuple2<Date, UniquePeople> post) throws Exception {
                for( long ids : RecommenderSystem.SELECTED_USERS ) {
                    if (post.f1.f1.containsKey(ids)){
                        return true;
                    }
                }
                return false;
            }
        }).flatMap(new FlatMapFunction<Tuple2<Date, UniquePeople>, Tuple3<Date, Long, Float[]> >() {
            @Override
            public void flatMap(Tuple2<Date, UniquePeople> post, Collector< Tuple3<Date, Long, Float[] > > collector) {
                HashMap<Long, Tuple2<Integer, HashSet<String>>> ids = new HashMap<>();
                for( int i = 0; i < RecommenderSystem.SELECTED_USERS.length; i++ ) {
                    long id = RecommenderSystem.SELECTED_USERS[i];
                    if (post.f1.f1.containsKey(id)){
                        ids.put( id, new Tuple2<>( i, post.f1.f1.get(id)) );
                    }
                }
                for( Map.Entry<Long, HashSet<String>> person: post.f1.f1.entrySet()  ) {
                    if ( ids.containsKey( person.getKey() ) ) {
                        continue; // People from the important set
                    }
                    Float[] rating = new Float[ RecommenderSystem.SELECTED_USERS.length ];
                    Arrays.fill(rating, 0.0f);
                    for( Map.Entry<Long, Tuple2< Integer,HashSet<String>>> important_person : ids.entrySet() ) {
                        for ( String role : person.getValue() ){
                            for ( String important_role : important_person.getValue().f1 ){
                                float val = hyperparams[ hyperparam_idx.get(role) ][ hyperparam_idx.get(important_role) ];
                                rating[important_person.getValue().f0] += val;
                            }
                        }
                    }
                    collector.collect( new Tuple3<>(post.f0, person.getKey(), rating) );
                }
            }
        }).keyBy(0,1).window(TumblingEventTimeWindows.of( this.tumblingSize )).aggregate(new AggregateFunction<Tuple3<Date, Long, Float[]>, Tuple3<Date, Long, Float[]>, Tuple3<Date, Long, Float[]>>() {
            @Override
            public Tuple3<Date, Long, Float[]> createAccumulator() {
                return new Tuple3<>();
            }

            @Override
            public Tuple3<Date, Long, Float[]> add(Tuple3<Date, Long, Float[]> input, Tuple3<Date, Long, Float[]> acc) {
                return merge(acc,input);
            }

            @Override
            public Tuple3<Date, Long, Float[]> getResult(Tuple3<Date, Long, Float[]> acc) {
                return acc;
            }

            @Override
            public Tuple3<Date, Long, Float[]> merge(Tuple3<Date, Long, Float[]> lhs, Tuple3<Date, Long, Float[]> rhs) {
                if ( lhs.f0 == null ) {
                    return rhs;
                }
                for ( int i = 0; i < lhs.f2.length; i++ ){
                    lhs.f2[i] += rhs.f2[i];
                }
                return lhs;
            }
        });

        return stream;
   }

}
