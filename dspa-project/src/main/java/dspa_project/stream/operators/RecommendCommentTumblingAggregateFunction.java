package dspa_project.stream.operators;

import dspa_project.model.CommentEvent;
import dspa_project.tasks.task2.RecommenderSystem;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class RecommendCommentTumblingAggregateFunction implements AggregateFunction<CommentEvent,
                                                                                    HashMap<Long, Tuple2<Float[], Integer>>,
                                                                                    HashMap<Long, Tuple2<Float[], Integer>>> {
    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> add(CommentEvent commentEvent, HashMap<Long, Tuple2<Float[], Integer>> activeUsers) {
        Float[] dynamicSimilarity = new Float[RecommenderSystem.SELECTED_USERS.length];
        for (int i = 0; i < RecommenderSystem.SELECTED_USERS.length; i++) {
            dynamicSimilarity[i] = 0.0f;
        }
        activeUsers.put(commentEvent.getPersonId(), new Tuple2<>(dynamicSimilarity, 1));
        return activeUsers;
    }

    @Override
    public HashMap<Long, Tuple2<Float[], Integer>> getResult(HashMap<Long, Tuple2<Float[], Integer>> activeUsers) {
        return activeUsers;
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
