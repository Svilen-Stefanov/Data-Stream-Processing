package dspa_project.stream.sources;

import dspa_project.model.LikeEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Iterator;
import java.util.stream.Stream;

public class LikeSourceFunction implements SourceFunction<LikeEvent> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<LikeEvent> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
