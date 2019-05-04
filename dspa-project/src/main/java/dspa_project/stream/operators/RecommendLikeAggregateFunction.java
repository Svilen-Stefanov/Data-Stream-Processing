package dspa_project.stream.operators;

import dspa_project.model.LikeEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;

public class RecommendLikeAggregateFunction implements AggregateFunction<LikeEvent, HashMap<Long, Float[]>, HashMap<Long, Float[]>> {
    //ProcessWindowFunction<LikeEvent, LikeEvent, Long, TimeWindow> {

    @Override
    public HashMap<Long, Float[]> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<Long, Float[]> add(LikeEvent likeEvent, HashMap<Long, Float[]> activeUsers) {
        Float[] dynamicSimilarity = new Float[10];
        activeUsers.put(likeEvent.getPersonId(), dynamicSimilarity);
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
