package dspa_project.stream.sources.operators;

import dspa_project.model.EventInterface;
import dspa_project.model.LikeEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class LikeProcessFunction extends ProcessFunction<Tuple2<Long, LikeEvent>, LikeEvent> {

    public static int count;
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
        count = 0;
    }

    @Override
    public void processElement(Tuple2<Long, LikeEvent> eventInterface, Context context, Collector<LikeEvent> collector) throws Exception {
        count++;
        Random rand = new Random();
        // timer for the delayed event
        long delayedEventTime = eventInterface.f1.getCreationDate().getTime() + getNormalDelay(rand);

        EventTimestamp current = state.value();
        if (current == null) {
            current = new EventTimestamp();
            current.eventInterface = eventInterface.f1;
        }

        current.delayedTime = context.timestamp();
        System.out.println(current.delayedTime);


        state.update(current);
        //context.timerService().registerProcessingTimeTimer(current.delayedTime + 5000);
        context.timerService().registerEventTimeTimer(current.delayedTime + 5000);//delayedEventTime
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LikeEvent> out) throws Exception {
        System.out.println("HERE");
        if (timestamp == state.value().delayedTime  + 5000) {
            out.collect(state.value().eventInterface);
            System.out.println(state.value().eventInterface.getId());
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

