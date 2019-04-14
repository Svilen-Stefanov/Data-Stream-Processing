/**
 * Based on LikeEventSource example in Ververica's Flink Training examples:
 *  https://github.com/ververica/flink-training-exercises/blob/292c7436198523e115e8a9839bd5eb3ee1c41999/
 *  src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/sources/LikeEventSource.java?
 *  fbclid=IwAR3NRxo0vBPIKXjJZMcckJ4SWUWwj5SvWYXFKGhJvsouIr6CxCnLi-q55m4
 */
package  dspa_project.stream.sources;

import dspa_project.model.EventInterface;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.lang.reflect.ParameterizedType;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class SimulationSourceFunction <Event extends EventInterface> implements SourceFunction<Event> {

    static String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    static String LOCAL_KAFKA_BROKER = "localhost:9092";


    private final long maxDelayMsecs;
    private final long watermarkDelayMSecs;
    private final String deserializer;

    private final String topic;
    private final double servingSpeed;

    private KafkaConsumer<String,Event> consumer;


    /**
     * Serves the Event records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param topic Topic name.
     * @param watermarkDelayMSecs Period for watermark
     * @param maxEventDelaySecs The max time in seconds by which events are delayed.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public SimulationSourceFunction(String topic, String deserializer, double maxEventDelaySecs, long watermarkDelayMSecs, double servingSpeedFactor) {
        if(maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.topic = topic;
        this.deserializer = deserializer;
        this.maxDelayMsecs = (int)(maxEventDelaySecs * 1000);
        this.watermarkDelayMSecs = maxDelayMsecs < watermarkDelayMSecs ? watermarkDelayMSecs : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        consumerProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.put("group.id", "test5");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", deserializer);
        consumerProps.put("max.poll.records", 1);

        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));

        generateUnorderedStream(sourceContext);

        consumer.close();
    }

    private Event readLike(){
        ConsumerRecords<String, Event> records = consumer.poll(5000);
        assert( records.count() == 1 || records.count() == 0 );
        for (ConsumerRecord<String, Event> record : records) {
            //System.out.println(record.value());
            return record.value();
        }
        return null;
    }

    private void generateUnorderedStream(SourceContext<Event> sourceContext) throws Exception {

        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        Random rand = new Random(7452);
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });

        Event like = readLike();
        if ( like != null ) {
            // extract starting timestamp
            dataStartTime = getEventTime(like);
            // get delayed time
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, like));
            // schedule next watermark
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        // peek at next ride
        Event old_like = like;
        like = readLike();
        if ( like == null) {
            like = old_like;
        }
        long last_now = -1;
        // read rides one-by-one and emit a random ride from the buffer each time
        while (emitSchedule.size() > 0) {

            // insert all events into schedule that might be emitted next
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long rideEventTime = like != null ? getEventTime(like) : -1;
            while(
                    like != null && ( // while there is a ride AND
                            emitSchedule.isEmpty() || // and no ride in schedule OR
                                    rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough rides in schedule
            )
            {
                // insert event into emit schedule
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, like));

                // read next ride
                like = readLike();
                if ( like != null ) {
                    rideEventTime = getEventTime(like);
                }
                else {
                    rideEventTime = -1;
                }
            }

            // emit schedule is updated, emit next element in schedule
            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;

            long now = Calendar.getInstance().getTimeInMillis();
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long waitTime = servingTime - now;

            Thread.sleep( (waitTime > 0) ? waitTime : 0);

            now = Calendar.getInstance().getTimeInMillis();

            if(head.f1 instanceof EventInterface) {
                Event emitLike = (Event) head.f1;

                /*System.out.println( "dRealTime: " + printDate(new Date(now-servingStartTime)) +
                                    " dScheduledTime: " + printDate(new Date(head.f0-dataStartTime)) +
                                    " noise: "+ printDate(new Date(head.f0 - getEventTime(emitLike)))
                                  );*/


                sourceContext.collectWithTimestamp(emitLike, getEventTime(emitLike));
            }
            else if(head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark)head.f1;
                // emit watermark
                sourceContext.emitWatermark(emitWatermark);

                /*System.out.println( "Watermark dRealTime: " + printDate(new Date(now-servingStartTime)) +
                        " dScheduledTime: " + printDate(new Date(head.f0-dataStartTime)) +
                        " dLastEvent: "+ printDate(new Date(emitWatermark.getTimestamp()-dataStartTime))
                );*/

                // schedule next watermark
                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
            }
            last_now = now;
        }
    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (long)(dataDiff / this.servingSpeed);
    }

    public long getEventTime(Event like) {
        return like.getCreationDate().getTime();
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs/2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(Math.abs(rand.nextGaussian()) * x);
        }
        return delay;
    }

    private String printDate(Date d){
        int milis = (int) (d.getTime() % 1000l);
        milis = milis<0 ? milis+1000 : milis;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DecimalFormat formater = new DecimalFormat("000");
        String milis_formated = formater.format(milis);
        return dateFormat.format(d) + ":" + milis_formated;
    }

    @Override
    public void cancel() {
        consumer.close();
    }

}