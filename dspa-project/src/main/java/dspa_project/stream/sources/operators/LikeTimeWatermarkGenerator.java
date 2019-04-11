package dspa_project.stream.sources.operators;

import dspa_project.model.LikeEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LikeTimeWatermarkGenerator  implements AssignerWithPunctuatedWatermarks<Tuple2<Long, LikeEvent>> {

    @Override
    public long extractTimestamp(Tuple2<Long, LikeEvent> event, long previousElementTimestamp) {
//        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//        Date date = new Date();
//        //System.out.println(); //2016/11/16 12:08:43
//        return date.getTime();
        return event.f1.getCreationDate().getTime();
    }

    @Override
    public Watermark checkAndGetNextWatermark(Tuple2<Long, LikeEvent> event, long extractedTimestamp) {
        // simply emit a watermark with every event
        return new Watermark(extractedTimestamp - 3000);
    }
}
