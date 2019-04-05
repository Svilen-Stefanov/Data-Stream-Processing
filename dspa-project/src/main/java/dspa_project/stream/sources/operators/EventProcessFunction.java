package dspa_project.stream.sources.operators;

import dspa_project.model.EventInterface;
import dspa_project.model.LikeEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class EventProcessFunction extends ProcessFunction<LikeEvent, LikeEvent> {

    // Class for the state to remember last timer set
    private class EventTimestamp {
        public LikeEvent eventInterface;
        public long delayedTime;
    }

    // delay in milliseconds
    private long maxRandomDelay = 60000;
    private long maxSpeedupFactor;
    private transient ValueState<EventTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", EventTimestamp.class));
    }

    @Override
    public void processElement(LikeEvent eventInterface, Context context, Collector<LikeEvent> collector) throws Exception {
        Random rand = new Random();
        // timer for the delayed event
        long delayedEventTime = eventInterface.getCreationDate().getTime() + getNormalDelay(rand);

        EventTimestamp current = state.value();
        if (current == null) {
            current = new EventTimestamp();
            current.eventInterface = eventInterface;
        }

        current.delayedTime = delayedEventTime;

        state.update(current);

        context.timerService().registerEventTimeTimer(delayedEventTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LikeEvent> out) throws Exception {
        // Stevan: does this mean to "close the window"
        // I think they were outing the <key, val> where you groupby the first one in the call of the process function
        if (timestamp == state.value().delayedTime) {
            out.collect(state.value().eventInterface);
        }
    }

    public long getNormalDelay(Random rand) {
        long delay = -1;
        long x = maxRandomDelay / 2;
        while(delay < 0 || delay > maxRandomDelay) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }
}
