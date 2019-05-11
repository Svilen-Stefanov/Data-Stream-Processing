package dspa_project.recommender_system;

import dspa_project.DataLoader;
import dspa_project.database.helpers.Graph;
import dspa_project.database.queries.SQLQuery;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

import static java.lang.Math.min;

public class RecommenderSystem {
    private final long tagRootNode = 0;
    // contains the graph for tagclasses hierarchy
    Graph tagSimilarityGraph;
    public static final int NUMBER_OF_RECOMMENDATIONS = 5;
    public static final long [] SELECTED_USERS = {554, 410, 830, 693, 254, 318, 139, 72, 916, 833};
    private float [][] possibleFriendsMap;
    private final float staticToDynamicSimilarityRatio = 0.5f;

    public RecommenderSystem(){
        DataLoader.parseStaticData();
        tagSimilarityGraph = new Graph(tagRootNode);
        evaluateSimilarity(410, 554);
        possibleFriendsMap = computeSimilarity();
    }

    private Vector<Tuple2<Long, Float>> tupleSort(Vector<Tuple2<Long, Float>> friends){
        for (int i = 0; i < friends.size(); i++) {
            for (int j = i; j > 0; j--) {
                if (friends.get(j).f0 < friends.get(j - 1).f1) {
                    Tuple2<Long, Float> temp = friends.get(j);
                    friends.set(j, friends.get(j - 1));
                    friends.set(j-1, temp);
                }
            }
        }
        return friends;
    }

    public Float[] getUserSimilarity(Long userID, Float[] dynamicSimilarity){
        Float[] totalSimilarityResult = new Float[SELECTED_USERS.length];
        Float totalSimilarity;
        for (int curSelectedUser = 0; curSelectedUser < SELECTED_USERS.length; curSelectedUser++) {
            float curUserStaticSimilarity = possibleFriendsMap[curSelectedUser][Math.toIntExact(userID)];
            totalSimilarity = 0f;
            // check if these people are already friends
            if (curUserStaticSimilarity > 0) {
                totalSimilarity = staticToDynamicSimilarityRatio * curUserStaticSimilarity
                        + (1 - staticToDynamicSimilarityRatio) * dynamicSimilarity[curSelectedUser];
            }

            totalSimilarityResult[curSelectedUser] = totalSimilarity;
        }

        return totalSimilarityResult;
    }

    public Vector<Vector<Tuple2<Long, Float>>> getSortedSimilarity(Iterable<Tuple2<Long, Float[]>> dynamicSimilarity) {
        // TODO Figure out will the dynamicSimilarities Be Computed Beforehand or within this method (and remove already existing friends)
        // TODO If we are computing it inside this method then we need a check (curUserStaticSimilarity > 0), otherwise they are friends already
        // TODO Fill with 5 in the beginning and then check for sorting. Otherwise get(4) will fail. (Insert them in sorted order)
        // TODO decide are dynamic and static similarities set from outside or passed as the argument
        // TODO: Test if it's working!

        // We create a 2D Matrix 10x5 dimensions.
        // Rows represent selected 10 users
        // Columns represent the top 5 recommended users
        // Each cell in the Matrix is Tuple<id of recommended user, similarity>

        HashMap<Long, Float[]> dynamicSimilarityMap = new HashMap<>();
        for (Tuple2<Long, Float[]> longTuple2: dynamicSimilarity) {
            dynamicSimilarityMap.put(longTuple2.f0, longTuple2.f1);
        }

        Vector<Vector<Tuple2<Long, Float>>> sortedFriends = new Vector<>(SELECTED_USERS.length);
        for (int curSelectedUser = 0; curSelectedUser < SELECTED_USERS.length; curSelectedUser++) {
            sortedFriends.add(curSelectedUser, new Vector<>(NUMBER_OF_RECOMMENDATIONS));
            for (int i = 0; i < NUMBER_OF_RECOMMENDATIONS; i++) {
                sortedFriends.get(curSelectedUser).add(i, new Tuple2<>());
            }
            for (Long onlineUser: dynamicSimilarityMap.keySet()) {
                float curUserStaticSimilarity = possibleFriendsMap[curSelectedUser][Math.toIntExact(onlineUser)];
                // check if these people are already friends
                if (curUserStaticSimilarity > 0){
                    Float totalSimilarity = staticToDynamicSimilarityRatio * curUserStaticSimilarity
                                            + (1 - staticToDynamicSimilarityRatio) * dynamicSimilarityMap.get(onlineUser)[curSelectedUser];
                    if (totalSimilarity > sortedFriends.get(curSelectedUser).get(4).f1) {
                        sortedFriends.get(curSelectedUser).set(4, new Tuple2<>(onlineUser, totalSimilarity));
                        sortedFriends.set(curSelectedUser, tupleSort(sortedFriends.get(curSelectedUser)));
                    }
                }
            }
        }

        return sortedFriends;
    }

    public float [][] computeSimilarity(){
        // TODO name this static similarity and return everything as it is always the same.
        // TODO use it to add with the dynamic similarity computed in 4h timeframes and then output top5
        int numberOfUsers = SQLQuery.getNumberOfPeople();
        float [][] possibleFriendsMap = new float[SELECTED_USERS.length][numberOfUsers];

        for (int i = 0; i < SELECTED_USERS.length; i++) {
            ArrayList<Long> possibleFriends = SQLQuery.getPossibleFriends(SELECTED_USERS[i]);
            for (int j = 0; j < possibleFriends.size(); j++) {
                int friendIdx = possibleFriends.get(j).intValue();
                possibleFriendsMap[i][friendIdx] = evaluateSimilarity(SELECTED_USERS[i], friendIdx);
            }
        }

        return possibleFriendsMap;
    }

    private float evaluateSimilarity(long a, long b){
        // TODO: change increment factors
        ArrayList<Long> tagsOfInterestA = SQLQuery.getTagsOfInterest(a);
        ArrayList<Long> tagsOfInterestB = SQLQuery.getTagsOfInterest(b);
        long tagClassA;
        long tagClassB;

        int tagFactor = 0;
        int tagClassFactor = 0;
        int workColleguesFactor = 0;
        int studyColleguesFactor = 0;
        int sameLanguage = 0;
        int sameLocation = 0;

        for (long tagA: tagsOfInterestA) {
            if (tagsOfInterestB.contains(tagA)) {
                tagFactor += 10;
            }
            else {
                tagClassA = SQLQuery.getTagClass(tagA);
                int minTagClassDistance = Integer.MAX_VALUE;
                for (long tagB: tagsOfInterestB) {
                    tagClassB = SQLQuery.getTagClass(tagB);
                    minTagClassDistance = min(minTagClassDistance, tagSimilarityGraph.distanceToCommonParent(tagClassA, tagClassB) );
                }
                tagClassFactor += tagSimilarityGraph.getMaxDepth() - minTagClassDistance;
            }
        }

        // check who is working at the same organization
        ArrayList<Long> workAtA = SQLQuery.getWorkAt(a);
        ArrayList<Long> workAtB = SQLQuery.getWorkAt(b);
        for (long workPlaceA: workAtA) {
            if (workAtB.contains(workPlaceA)) {
                workColleguesFactor += 10;
            }
        }

        // check who is studying at the same organization
        long [] studyAtA = SQLQuery.getUniversity(a);
        long [] studyAtB = SQLQuery.getUniversity(b);
        if(studyAtA[0] == studyAtB[0]) {
            studyColleguesFactor += 5;
            if (studyAtA[1] == studyAtB[1])
                studyColleguesFactor += 5;
        }

        ArrayList<String> speaksLanguageA = SQLQuery.getLanguage(a);
        ArrayList<String> speaksLanguageB = SQLQuery.getLanguage(b);
        for (String lanuageA: speaksLanguageA) {
            if (speaksLanguageB.contains(lanuageA)) {
                sameLanguage += 10;
            }
        }

        long locationA = SQLQuery.getLocation(a);
        long locationB = SQLQuery.getLocation(b);
        if(locationA == locationB) {
            sameLocation += 5;
        }


        return similarityMetric(tagFactor, tagClassFactor, workColleguesFactor, studyColleguesFactor, sameLanguage, sameLocation);
    }

    private float similarityMetric( int tagFactor, int tagClassFactor, int workCollegueFactor, int studyCollegueFactor, int sameLanguage, int sameLocation){
        // TODO: tagClassFactor is accumulative for all not exact interests and should have a small weighting factor
        return (tagFactor + tagClassFactor + workCollegueFactor + studyCollegueFactor + sameLanguage + sameLocation) / 1.0f;
    }

    /*
    * TODO:
    *   - refactor SQL queries (discuss -> check suggestions)
    *   - define similarity metric
    *   - LAST PRIORITY: add functionality for similarity : same (common) forums / age / current job
    *
    *   Unusual activity:
    *   - comparison between static and dynamic data: UK (103) and England (28), not in place is part of place
    *   - for each post/event, we can check if it corresponds to the static data (place_isLocatedIn_place)
    * */
}
