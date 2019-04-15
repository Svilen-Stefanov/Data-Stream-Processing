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
        int minTagClassDistance = Int.MaxValue();
        for (long tagA: tagsOfInterestA) {
            if (tagsOfInterestB.contains(tagA)) {
                tagFactor += 10;
            }
            else {
                tagClassA = SQLQuery.getTagClass(tagA);
                for (long tagB: tagsOfInterestB) {
                    tagClassB = SQLQuery.getTagClass(tagB);
                    minTagClassDistance = min(minTagClassDistance, tagSimilarityGraph.distanceToCommonParent(tagClassA, tagClassB) );
                }
                tagClassFactor += tagSimilarityGraph.getMaxDepth() - minTagClassDistance;
            }
        }

        return similarityMetric(tagFactor, tagClassFactor);
    }

    private float similarityMetric( int tagFactor, int tagClassFactor){
        return (tagFactor + tagClassFactor) / 1.0f;
    }
}
