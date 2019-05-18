package dspa_project.tasks.task1;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class UniquePeoplePostCollection extends HashMap<Long, HashMap<Long, HashSet<String>>> {
    public UniquePeoplePostCollection(){
        super();
    }
    public UniquePeoplePostCollection(int initialCapacity) {
        super(initialCapacity);
    }
    public UniquePeoplePostCollection(int initialCapacity, float loadFactor){
        super(initialCapacity,loadFactor);
    }
    public UniquePeoplePostCollection(Map<? extends Long,? extends HashMap<Long, HashSet<String>>> m){
        super(m);
    }
}
