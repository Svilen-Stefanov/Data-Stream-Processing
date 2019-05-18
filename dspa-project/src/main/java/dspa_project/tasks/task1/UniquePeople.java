package dspa_project.tasks.task1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.HashSet;

public class UniquePeople extends Tuple2<Long, HashMap<Long, HashSet<String>>> {
    public UniquePeople(Long value1, HashMap<Long, HashSet<String>> value2) {
        super(value1,value2);
    }
    public UniquePeople(){
        super();
    }
}
