package dspa_project.tasks.task2;

import dspa_project.database.helpers.Graph;
import dspa_project.database.queries.SQLQuery;
import dspa_project.model.Person;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

import static java.lang.Math.min;

public class RecommenderSystem {
    private final long tagRootNode = 0;
    private int numberOfUsers;
    // contains the graph for tagclasses hierarchy
    Graph tagSimilarityGraph;
    public static ArrayList<Person> people;
    private HashMap<Long, Long> tagClassMap;
    public static final int NUMBER_OF_RECOMMENDATIONS = 5;
    public static final long [] SELECTED_USERS = {554, 410, 830, 693, 254, 318, 139, 72, 916, 833};
    public static final HashMap<Integer, Integer> ID_TO_IDX = new HashMap<Integer, Integer>() {{
        put(554, 0);
        put(410, 1);
        put(830, 2);
        put(693, 3);
        put(254, 4);
        put(318, 5);
        put(139, 6);
        put(72, 7);
        put(916, 8);
        put(833, 9);
    }};
    private static float [][] possibleFriendsMap;
    private static final float staticToDynamicSimilarityRatio = 0.5f;


    public RecommenderSystem(){
        tagSimilarityGraph = new Graph(tagRootNode);

        possibleFriendsMap = SQLQuery.getStaticSimilarity();

        // compute and store static similarity if not stored in the database
        if (possibleFriendsMap == null) {
            tagClassMap = SQLQuery.getTagClasses();

            numberOfUsers = SQLQuery.getNumberOfPeople();

            people = new ArrayList<>();
            for (int i = 0; i < numberOfUsers; i++) {
                Person person = new Person(i);
                people.add(person);
            }

            possibleFriendsMap = computeStaticSimilarity();
            SQLQuery.createStaticSimilarityTable(possibleFriendsMap);
        }

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

    public static Float[] getUserSimilarity(Tuple2<Long, Float[]> dynamicSimilarity){
        Float[] totalSimilarityResult = new Float[SELECTED_USERS.length];
        Float totalSimilarity;
        for (int curSelectedUser = 0; curSelectedUser < SELECTED_USERS.length; curSelectedUser++) {
            float curUserStaticSimilarity = possibleFriendsMap[curSelectedUser][Math.toIntExact(dynamicSimilarity.f0)];
            totalSimilarity = 0f;
            // check if these people are already friends
            if (curUserStaticSimilarity > 0) {
                totalSimilarity = staticToDynamicSimilarityRatio * curUserStaticSimilarity
                        + (1 - staticToDynamicSimilarityRatio) * dynamicSimilarity.f1[curSelectedUser];
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

    public float [][] computeStaticSimilarity(){
        float [][] possibleFriendsMap = new float[SELECTED_USERS.length][numberOfUsers];

        for (int i = 0; i < SELECTED_USERS.length; i++) {
            System.out.println(i);
            ArrayList<Long> possibleFriends = SQLQuery.getPossibleFriends(SELECTED_USERS[i]);
            for (int j = 0; j < possibleFriends.size(); j++) {
                int friendIdx = possibleFriends.get(j).intValue();
                possibleFriendsMap[i][friendIdx] = evaluateSimilarity(SELECTED_USERS[i], friendIdx);
            }
        }

        return possibleFriendsMap;
    }

    private float evaluateSimilarity(long a, long b){
        Person personA = people.get( (int)a );
        Person personB = people.get( (int)b );

        ArrayList<Long> tagsOfInterestA = personA.getTagsOfInterest();
        ArrayList<Long> tagsOfInterestB = personB.getTagsOfInterest();
        long tagClassA;
        long tagClassB;

        float tagFactor = 0;
        float workColleguesFactor = 0;
        float studyColleguesFactor = 0;
        float sameLanguage = 0;
        float sameLocation = 0;

        for (long tagA: tagsOfInterestA) {
            if (tagsOfInterestB.contains(tagA)) {
                tagFactor += 1.0f;
            }
            else {
                tagClassA = SQLQuery.getTagClass(tagA);
                int minTagClassDistance = Integer.MAX_VALUE;
                for (long tagB: tagsOfInterestB) {
                    tagClassB = tagClassMap.get(tagB);
                    minTagClassDistance = min(minTagClassDistance, tagSimilarityGraph.distanceToCommonParent(tagClassA, tagClassB) );
                }

                if (minTagClassDistance != Integer.MAX_VALUE) {
                    float similarity = (tagSimilarityGraph.getMaxDepth() - minTagClassDistance - 1) / (1.0f * tagSimilarityGraph.getMaxDepth());
                    tagFactor += similarity > 0 ? similarity : 0;
                }
            }
        }

        if (tagsOfInterestA.size() != 0)
            tagFactor /= tagsOfInterestA.size();

        // check who is working at the same organization
        ArrayList<Long> workAtA = personA.getWorkAt();
        ArrayList<Long> workAtB = personB.getWorkAt();
        for (long workPlaceA: workAtA) {
            if (workAtB.contains(workPlaceA)) {
                workColleguesFactor += 1.0f;
            }
        }

        if (min(workAtA.size(), workAtB.size()) != 0)
            workColleguesFactor /= min(workAtA.size(), workAtB.size());

        // check who is studying at the same organization
        ArrayList<Long> studyAtA = personA.getStudyAt();
        ArrayList<Long> studyAtB = personB.getStudyAt();
        if(!studyAtA.isEmpty() && !studyAtB.isEmpty() && studyAtA.get(0).equals(studyAtB.get(0))) {
            studyColleguesFactor += 1.0f;
            if (studyAtA.get(1).equals(studyAtB.get(1)))
                studyColleguesFactor += 1.0f;
        }
        studyColleguesFactor /= 2.0f;

        ArrayList<String> speaksLanguageA = personA.getSpeaksLanguage();
        ArrayList<String> speaksLanguageB = personB.getSpeaksLanguage();
        for (String lanuageA: speaksLanguageA) {
            if (speaksLanguageB.contains(lanuageA)) {
                sameLanguage += 1.0f;
            }
        }

        if (min(speaksLanguageA.size(), speaksLanguageB.size()) != 0)
            sameLanguage /= min(speaksLanguageA.size(), speaksLanguageB.size());

        long locationA = personA.getLocation();
        long locationB = personB.getLocation();
        if(locationA == locationB) {
            sameLocation = 1.0f;
        }

        return similarityMetric(tagFactor, workColleguesFactor, studyColleguesFactor, sameLanguage, sameLocation);
    }

    private float similarityMetric( float tagFactor, float workCollegueFactor, float studyCollegueFactor, float sameLanguage, float sameLocation){
        return (tagFactor + workCollegueFactor + studyCollegueFactor + sameLanguage + sameLocation) / 5.0f;
    }
}
