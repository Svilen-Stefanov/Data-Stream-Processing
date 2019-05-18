package dspa_project.stream.operators;

import dspa_project.tasks.task2.RecommenderSystem;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.Vector;

public class SimilarityAggregateFunction implements AggregateFunction<  Tuple2<Long, Float[]>,
                                                                        Vector<Vector<Tuple2<Long, Float>>>,
                                                                        Vector<Vector<Tuple2<Long, Float>>>>{

    @Override
    public Vector<Vector<Tuple2<Long, Float>>> createAccumulator() {
        Vector<Vector<Tuple2<Long, Float>>> sortedFriends = new Vector<>(RecommenderSystem.SELECTED_USERS.length);
        for (int curSelectedUser = 0; curSelectedUser < RecommenderSystem.SELECTED_USERS.length; curSelectedUser++) {
            sortedFriends.add(curSelectedUser, new Vector<>(RecommenderSystem.NUMBER_OF_RECOMMENDATIONS));
            for (int i = 0; i < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS; i++) {
                sortedFriends.get(curSelectedUser).add(i, new Tuple2<>());
            }
        }

        return sortedFriends;
    }

    @Override
    public Vector<Vector<Tuple2<Long, Float>>> add(Tuple2<Long, Float[]> longTuple2, Vector<Vector<Tuple2<Long, Float>>> vectors) {
        // add static similarity
        Float[] totalSimilarity = RecommenderSystem.getUserSimilarity(longTuple2);

        for (int curSelectedUser = 0; curSelectedUser < RecommenderSystem.SELECTED_USERS.length; curSelectedUser++) {
            for (int i = 0; i < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS; i++) {
                if (vectors.get(curSelectedUser).get(i).f1 < totalSimilarity[curSelectedUser]){
                    vectors.get(curSelectedUser).add(i, new Tuple2<>(longTuple2.f0, totalSimilarity[curSelectedUser]));
                    break;
                }
            }
            if (vectors.get(curSelectedUser).size() > RecommenderSystem.NUMBER_OF_RECOMMENDATIONS){
                vectors.get(curSelectedUser).remove(RecommenderSystem.NUMBER_OF_RECOMMENDATIONS);
            }
        }
        return vectors;
    }

    @Override
    public Vector<Vector<Tuple2<Long, Float>>> getResult(Vector<Vector<Tuple2<Long, Float>>> vectors) {
        return vectors;
    }

    @Override
    public Vector<Vector<Tuple2<Long, Float>>> merge(Vector<Vector<Tuple2<Long, Float>>> vectors, Vector<Vector<Tuple2<Long, Float>>> acc1) {
        Vector<Vector<Tuple2<Long, Float>>> result = new Vector<>(RecommenderSystem.SELECTED_USERS.length);
        for (int curSelectedUser = 0; curSelectedUser < RecommenderSystem.SELECTED_USERS.length; curSelectedUser++) {
            result.add(curSelectedUser, new Vector<>(RecommenderSystem.NUMBER_OF_RECOMMENDATIONS));
            Vector<Tuple2<Long, Float>> currentRow =  result.get(curSelectedUser);

            int i1 = 0;
            int i2 = 0;
            Tuple2<Long, Float> t2;
            for (int i = 0; i < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS; i++) {
                if(vectors.get(curSelectedUser).get(i1).f1 >= acc1.get(curSelectedUser).get(i2).f1){
                    t2 = vectors.get(curSelectedUser).get(i1);
                    i1++;
                }
                else  {
                    t2 = acc1.get(curSelectedUser).get(i2);
                    i2++;
                }

                currentRow.add(i, t2);
            }

//            for (int i = 0; i < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS; i++) {
//                addNewEntry(currentRow, vectors.get(curSelectedUser).get(i));
//                addNewEntry(currentRow, acc1.get(curSelectedUser).get(i));
//            }
        }
        return result;
    }


    // Legacy code
    private void addNewEntry(Vector<Tuple2<Long, Float>> currentRow, Tuple2<Long, Float> tuple2s) {
        if (currentRow.size() < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS){
            // Insert ordered
            for (int i = 0; i < currentRow.size(); i++) {
                if (currentRow.get(i).f1 < tuple2s.f1){
                    currentRow.add(i, tuple2s);
                    return;
                }
            }
            if (currentRow.size() < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS){
                currentRow.add(currentRow.size(), tuple2s); // -1
            }
        }
        else{
            // Check if it should be inserted
            for (int i = 0; i < currentRow.size(); i++) {
                if (currentRow.get(i).f1 < tuple2s.f1){
                    currentRow.add(i, tuple2s);
                    break;
                }
            }
            if (currentRow.size() > RecommenderSystem.NUMBER_OF_RECOMMENDATIONS){
                currentRow.remove(RecommenderSystem.NUMBER_OF_RECOMMENDATIONS);
            }
        }
    }
}
