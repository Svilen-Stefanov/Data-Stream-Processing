package dspa_project.tasks.task1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;

public class UniquePeople extends Tuple2<Long, HashSet<Long>> {
    public UniquePeople(Long value1, HashSet<Long> value2) {
        super(value1,value2);
    }
    public UniquePeople(){
        super();
    }
}
