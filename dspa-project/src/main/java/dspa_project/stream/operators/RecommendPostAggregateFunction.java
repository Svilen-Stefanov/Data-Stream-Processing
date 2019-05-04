package dspa_project.stream.operators;

import dspa_project.model.PostEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;

public class RecommendPostAggregateFunction implements AggregateFunction<PostEvent, HashMap<Long, Float[]>, HashMap<Long, Float[]>> {
    @Override
    public HashMap<Long, Float[]> createAccumulator() {
        return new HashMap<Long, Float[]>();
    }

    @Override
    public HashMap<Long, Float[]> add(PostEvent postEvent, HashMap<Long, Float[]> activeUsers) {
        Float[] dynamicSimilarity = new Float[10];
        activeUsers.put(postEvent.getPersonId(), dynamicSimilarity);
        return activeUsers;
    }

    @Override
    public HashMap<Long, Float[]> getResult(HashMap<Long, Float[]> activeUsers) {
        return activeUsers;
    }

    @Override
    public HashMap<Long, Float[]> merge(HashMap<Long, Float[]> hs1, HashMap<Long, Float[]> hs2) {
        hs1.putAll(hs2);
        return hs1;
    }
}
