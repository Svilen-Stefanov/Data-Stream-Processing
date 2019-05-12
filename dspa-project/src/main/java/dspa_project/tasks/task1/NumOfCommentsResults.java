package dspa_project.tasks.task1;

import java.util.HashMap;
import java.util.Map;

public class NumOfCommentsResults extends HashMap<Long, Integer> {
    public NumOfCommentsResults(){
        super();
    }
    public NumOfCommentsResults(int initialCapacity) {
        super(initialCapacity);
    }
    public NumOfCommentsResults(int initialCapacity, float loadFactor){
        super(initialCapacity,loadFactor);
    }
    public NumOfCommentsResults(Map<? extends Long,? extends Integer> m){
        super(m);
    }
}