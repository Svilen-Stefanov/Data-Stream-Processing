package dspa_project.tasks.task1;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Date;

public class CountingResults extends Tuple2<Date, PostsCounts> {
    public CountingResults(Date value0,
                  PostsCounts value1) {
        super(value0,value1);
    }
    public CountingResults(){
        super();
    }

}