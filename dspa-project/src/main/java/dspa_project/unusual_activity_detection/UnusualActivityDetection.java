package dspa_project.unusual_activity_detection;

import dspa_project.database.queries.SQLQuery;

public class UnusualActivityDetection {
    public boolean checkLocation(long userId, long locationId){
        long loc = SQLQuery.getLocation(userId);
        long rootUser = SQLQuery.getRootLocation(loc);
        long rootEvent = SQLQuery.getRootLocation(locationId);
        return rootEvent == rootUser;
    }
}
