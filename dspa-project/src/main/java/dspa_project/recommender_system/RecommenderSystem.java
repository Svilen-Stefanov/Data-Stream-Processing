package dspa_project.recommender_system;

import dspa_project.DataLoader;
import dspa_project.database.helpers.Graph;
import dspa_project.database.queries.SQLQuery;
import dspa_project.model.Person;
import scala.Int;

import java.util.ArrayList;
import java.util.HashMap;

import static java.lang.Math.min;

public class RecommenderSystem {
    private final long tagRootNode = 0;
    // contains the graph for tagclasses hierarchy
    Graph tagSimilarityGraph;
    private final long [] selectedUsers = {554, 410, 830, 693, 254, 318, 139, 72, 916, 833};

    public RecommenderSystem(){
        DataLoader.parseStaticData();
        tagSimilarityGraph = new Graph(tagRootNode);
    }

    public float computeSimilarity(){
        int numberOfUsers = SQLQuery.getNumberOfPeople();
        float [][] possibleFriendsMap = new float[selectedUsers.length][numberOfUsers];

        for (int i = 0; i < selectedUsers.length; i++) {
            ArrayList<Long> possibleFriends = SQLQuery.getPossibleFriends(selectedUsers[i]);
            for (int j = 0; j < possibleFriends.size(); j++) {
                int friendIdx = possibleFriends.get(j).intValue();
                possibleFriendsMap[i][friendIdx] = evaluateSimilarity(selectedUsers[i], friendIdx);
            }
        }

        return 0;
    }

    private float evaluateSimilarity(long a, long b){
        ArrayList<Long> tagsOfInterestA = SQLQuery.getTagsOfInterest(a);
        ArrayList<Long> tagsOfInterestB = SQLQuery.getTagsOfInterest(b);
        long tagClassA;
        long tagClassB;

        float similarity;
        int tagFactor = 0;
        int tagClassFactor = 0;
        int workColleguesFactor = 0;
        int studyColleguesFactor = 0;

        for (long tagA: tagsOfInterestA) {
            if (tagsOfInterestB.contains(tagA)) {
                tagFactor += 10;
            }
            else {
                tagClassA = SQLQuery.getTagClass(tagA);
                int minTagClassDistance = Int.MaxValue();
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


        return similarityMetric(tagFactor, tagClassFactor, workColleguesFactor, studyColleguesFactor);
    }

    private float similarityMetric( int tagFactor, int tagClassFactor, int workCollegueFactor, int studyCollegueFactor){
        // TODO: tagClassFactor is accumulative for all not exact interests and should have a small weighting factor
        return (tagFactor + tagClassFactor + workCollegueFactor + studyCollegueFactor) / 1.0f;
    }

    /*
    * TODO:
    *   - refactor SQL queries
    *   - refactor data loading
    *   - sort similarity for users (finish evaluateSimilarity)
    *   - add functionality for similarity : speaks language / same (common) forums / location / age / current job
    *   - define similarity metric
    * */
}
