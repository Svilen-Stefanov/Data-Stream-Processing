package dspa_project.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class RecommendEventAggregateAllFunction implements AggregateFunction<HashMap<Long, Tuple2<Float[], Integer>>,
                                                                               HashMap<Long, Tuple2<Float[], Integer>>,
                                                                               HashMap<Long, Tuple2<Float[], Integer>>> {

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> add(HashMap<Long, Tuple2<Float[], Integer>> likes, HashMap<Long, Tuple2<Float[], Integer>> acc) {
        return merge(acc, likes);
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> getResult(HashMap<Long, Tuple2<Float[], Integer>> acc) {
        return acc;
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> merge(HashMap<Long, Tuple2<Float[], Integer>> lhs, HashMap<Long, Tuple2<Float[], Integer>> rhs) {
        for ( Long key : rhs.keySet() ) {
            if ( lhs.containsKey(key) ) {
                Float[] mergedSum = new Float[lhs.get(key).f0.length];
                for (int i = 0; i < mergedSum.length; i++) {
                    mergedSum[i] = lhs.get(key).f0[i] + rhs.get(key).f0[i];
                }

                lhs.put( key, new Tuple2<>( mergedSum, lhs.get(key).f1 + rhs.get(key).f1));
            } else {
                lhs.put( key, rhs.get(key) );
            }
        }
        return lhs;
    }
}
