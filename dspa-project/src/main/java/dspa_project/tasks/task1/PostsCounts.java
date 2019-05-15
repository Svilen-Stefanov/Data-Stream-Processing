package dspa_project.tasks.task1;

import java.util.HashMap;
import java.util.Map;

public class PostsCounts extends HashMap<Long, Integer> {
    public PostsCounts(){
        super();
    }
    public PostsCounts(int initialCapacity) {
        super(initialCapacity);
    }
    public PostsCounts(int initialCapacity, float loadFactor){
        super(initialCapacity,loadFactor);
    }
    public PostsCounts(Map<? extends Long,? extends Integer> m){
        super(m);
    }
}