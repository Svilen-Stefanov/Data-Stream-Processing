package dspa_project.tasks.task3;

import dspa_project.database.queries.SQLQuery;

import java.util.ArrayList;
import java.util.Collections;
import static java.lang.Math.max;

public class UnusualActivityDetection {
    public static boolean checkFraud(long userId, long locationId){

        //notify for strange behavior if access from a different continent
        long loc = SQLQuery.getLocation(userId);
        long rootUser = SQLQuery.getRootLocation(loc);
        long rootEvent = SQLQuery.getRootLocation(locationId);
        return rootEvent != rootUser;

        /*
        * The commented out code is used to check how different 2 locations are
        * E.g it can check if the country or a given district is the same (not only the continent as above)
        * */
        /*
        ArrayList<Long> userLocationTree = SQLQuery.getLocationTree(loc);
        ArrayList<Long> eventLocationTree = SQLQuery.getLocationTree(locationId);
        Collections.reverse(userLocationTree);
        Collections.reverse(eventLocationTree);
        int maxDepth = max(eventLocationTree.size(), userLocationTree.size()) - 1;
        int curDepth = 0;
        boolean sameContinents = false;
        for ( ; curDepth < maxDepth; curDepth++) {
            if(curDepth > eventLocationTree.size() - 1 || curDepth > userLocationTree.size() - 1 || !userLocationTree.get(curDepth).equals(eventLocationTree.get(curDepth))){
                break;
            }
            sameContinents = true;
        }

        return !sameContinents;  // curDepth > 0  -- users have more in common then just the continent
        */
    }
}
