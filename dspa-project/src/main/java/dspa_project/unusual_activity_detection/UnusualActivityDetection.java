package dspa_project.unusual_activity_detection;

import dspa_project.database.queries.SQLQuery;

import java.util.ArrayList;
import java.util.Collections;

import static java.lang.Math.max;

public class UnusualActivityDetection {
    public boolean checkLocation(long userId, long locationId){
        long loc = SQLQuery.getLocation(userId);
        // long rootUser = SQLQuery.getRootLocation(loc);
        // long rootEvent = SQLQuery.getRootLocation(locationId);
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
        // return rootEvent == rootUser;

        // notify for strange behavior if access from a different continent
        // TODO: if you wanna check the continents only, you can use getRootLocation (see commented out code)
        // TODO: idea was to just have a depth if we need it
        return sameContinents;  // curDepth > 0  -- users have more in common then just the continent
    }
}
