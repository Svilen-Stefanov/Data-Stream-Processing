package dspa_project.tasks.task2;

import dspa_project.database.helpers.Graph;
import dspa_project.database.queries.SQLQuery;
import dspa_project.model.Person;
import me.tongfei.progressbar.ProgressBar;
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
    public static final long [] SELECTED_USERS = {410, 554, 830, 693, 254, 318, 139, 72, 916, 833};
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
            ProgressBar pb = new ProgressBar("Creating people objects", numberOfUsers);
            pb.start();
            for (int i = 0; i < numberOfUsers; i++) {
                Person person = new Person(i);
                people.add(person);
                pb.step();
            }
            pb.stop();

            possibleFriendsMap = computeStaticSimilarity();
            SQLQuery.createStaticSimilarityTable(possibleFriendsMap);
        }

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

    private float [][] computeStaticSimilarity(){
        float [][] possibleFriendsMap = new float[SELECTED_USERS.length][numberOfUsers];

        ProgressBar pb = new ProgressBar("Computing static similarity", numberOfUsers*SELECTED_USERS.length);
        pb.start();
        for (int i = 0; i < SELECTED_USERS.length; i++) {
            ArrayList<Long> possibleFriends = SQLQuery.getPossibleFriends(SELECTED_USERS[i]);
            for (int j = 0; j < possibleFriends.size(); j++) {
                pb.step();
                int friendIdx = possibleFriends.get(j).intValue();
                possibleFriendsMap[i][friendIdx] = evaluateSimilarity(SELECTED_USERS[i], friendIdx);
            }
            pb.stepBy(numberOfUsers - possibleFriends.size());
        }
        pb.stop();

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
