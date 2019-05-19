package dspa_project.stream.operators;

import dspa_project.tasks.task2.RecommenderSystem;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
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
                sortedFriends.get(curSelectedUser).add(i, new Tuple2<>(0L, 0.0f));
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
                    if (vectorContainsTuple(vectors.get(curSelectedUser), longTuple2))
                        break;
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

    private boolean vectorContainsTuple(Vector<Tuple2<Long, Float>> vectors, Tuple2<Long, Float[]> longTuple2) {
        for (int i = 0; i < vectors.size(); i++) {
            if (vectors.get(i).f0 == longTuple2.f0)
                return true;
        }
        return false;
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
            Tuple2<Long, Float> tupleToInsert = new Tuple2<>(0L, 0F);
            ArrayList<Long> currentlyInserted = new ArrayList<>();
            for (int i = 0; i < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS; i++) {
                boolean inserted = false;
                while ( !inserted && (i1 < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS || i2 < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS) ){
                    Long u1 = 0L;
                    Long u2 = 0L;
                    Float v1 = 0f;
                    Float v2 = 0f;

                    if (i1 < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS){
                        u1 = vectors.get(curSelectedUser).get(i1).f0;
                        v1 = vectors.get(curSelectedUser).get(i1).f1;
                    }

                    if (i1 < RecommenderSystem.NUMBER_OF_RECOMMENDATIONS){
                        u2 = vectors.get(curSelectedUser).get(i2).f0;
                        v2 = vectors.get(curSelectedUser).get(i2).f1;
                    }

                    if(v1 >= v2){
                        if (!currentlyInserted.contains(u1)) {
                            tupleToInsert = vectors.get(curSelectedUser).get(i1);
                            inserted = true;
                        }
                        i1++;
                    }
                    else  {
                        if (!currentlyInserted.contains(u2)) {
                            tupleToInsert = acc1.get(curSelectedUser).get(i2);
                            inserted = true;
                        }
                        i2++;
                    }
                }

                currentRow.add(i, tupleToInsert);
                currentlyInserted.add(tupleToInsert.f0);
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
