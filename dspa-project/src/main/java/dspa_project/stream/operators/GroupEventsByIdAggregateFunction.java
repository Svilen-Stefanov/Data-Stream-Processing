package dspa_project.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class GroupEventsByIdAggregateFunction implements AggregateFunction<  Tuple2<Long, Float[]>,
                                                                        HashMap<Long, Tuple2<Float[], Integer>>,
                                                                        HashMap<Long, Tuple2<Float[], Integer>>> {
    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> add(Tuple2<Long, Float[]> incomingSimilarity, HashMap<Long, Tuple2<Float[], Integer>> activeUsers) {
        Long key = incomingSimilarity.f0;
        if ( activeUsers.containsKey(key) ) {
            Float[] mergedSum = new Float[incomingSimilarity.f1.length];
            for (int i = 0; i < mergedSum.length; i++) {
                mergedSum[i] = incomingSimilarity.f1[i] + activeUsers.get(key).f0[i];
            }

            activeUsers.put( key, new Tuple2<>( mergedSum, activeUsers.get(key).f1++));
        }
        else {
            activeUsers.put( key, new Tuple2<>(incomingSimilarity.f1, 1) );
        }

        return activeUsers;
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> getResult(HashMap<Long, Tuple2<Float[], Integer>> activeUsers) {
        return activeUsers;
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> merge(HashMap<Long, Tuple2<Float[], Integer>> hs1, HashMap<Long, Tuple2<Float[], Integer>> hs2) {
        for ( Long key : hs2.keySet() ) {
            if ( hs1.containsKey(key) ) {
                Float[] mergedSum = new Float[hs1.get(key).f0.length];
                for (int i = 0; i < mergedSum.length; i++) {
                    mergedSum[i] = hs1.get(key).f0[i] + hs2.get(key).f0[i];
                }

                hs1.put( key, new Tuple2<>( mergedSum, hs1.get(key).f1 + hs2.get(key).f1));
            } else {
                hs1.put( key, hs2.get(key) );
            }
        }

        return hs1;
    }
}
