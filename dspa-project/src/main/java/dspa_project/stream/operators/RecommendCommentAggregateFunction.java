package dspa_project.stream.operators;

import dspa_project.model.CommentEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;

public class RecommendCommentAggregateFunction implements AggregateFunction<CommentEvent, HashMap<Long, Float>, HashMap<Long, Float>> {
    @Override
    public HashMap<Long, Float> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<Long, Float> add(CommentEvent commentEvent, HashMap<Long, Float> activeUsers) {
        float dynamicSimilarity = 1.0f;
        activeUsers.put(commentEvent.getPersonId(), dynamicSimilarity);
        return activeUsers;
    }

    @Override
    public HashMap<Long, Float> getResult(HashMap<Long, Float> activeUsers) {
        return activeUsers;
    }

    @Override
    public HashMap<Long, Float> merge(HashMap<Long, Float> hs1, HashMap<Long, Float> hs2) {
        hs1.putAll(hs2);
        return hs1;
    }
}
