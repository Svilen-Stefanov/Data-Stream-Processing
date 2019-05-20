package dspa_project.tasks.task3;

import dspa_project.config.ConfigLoader;
import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;
import dspa_project.model.PostEvent;
import dspa_project.stream.sinks.WriteOutputFormat;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Task3 {

    public Task3( StreamExecutionEnvironment env ) {
        SourceFunction<CommentEvent> sourceFraudComments = new SimulationSourceFunction<CommentEvent>("Task3","comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);

        SourceFunction<PostEvent> sourceFraudPosts = new SimulationSourceFunction<PostEvent>("Task3","post-topic", "dspa_project.schemas.PostSchema",
                2, 10000, 10000);

        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);

        DataStream<Tuple2<EventInterface, Boolean>> fraudComments = env.addSource(sourceFraudComments, typeInfoComments)
                .map(new MapFunction<CommentEvent, Tuple2<EventInterface, Boolean>>() {
                    @Override
                    public Tuple2<EventInterface, Boolean> map(CommentEvent commentEvent) throws Exception {
                        boolean fraud = UnusualActivityDetection.checkFraud(commentEvent.getPersonId(), commentEvent.getPlaceId());
                        return new Tuple2<>(commentEvent, fraud);
                    }
                })
                .filter(new FilterFunction<Tuple2<EventInterface, Boolean>>() {
                    @Override
                    public boolean filter(Tuple2<EventInterface, Boolean> commentEventTuple2) throws Exception {
                        return commentEventTuple2.f1;
                    }
                });

        DataStream<Tuple2<EventInterface, Boolean>> fraudPosts = env.addSource(sourceFraudPosts, typeInfoPosts)
                .map(new MapFunction<PostEvent, Tuple2<EventInterface, Boolean>>() {
                    @Override
                    public Tuple2<EventInterface, Boolean> map(PostEvent postEvent) throws Exception {
                        boolean fraud = UnusualActivityDetection.checkFraud(postEvent.getPersonId(), postEvent.getPlaceId());
                        return new Tuple2<>(postEvent, fraud);
                    }
                })
                .filter(new FilterFunction<Tuple2<EventInterface, Boolean>>() {
                    @Override
                    public boolean filter(Tuple2<EventInterface, Boolean> posttEventTuple2) throws Exception {
                        return posttEventTuple2.f1;
                    }
                });

        DataStream<String> allFrauds = fraudComments.union(fraudPosts)
                .map(new MapFunction<Tuple2<EventInterface, Boolean>, String>() {
                    @Override
                    public String map(Tuple2<EventInterface, Boolean> stringBooleanTuple2) throws Exception {
                        String personId = String.valueOf(stringBooleanTuple2.f0.getPersonId());
                        String creationDate = String.valueOf(stringBooleanTuple2.f0.getCreationDate());
                        String id = String.valueOf(stringBooleanTuple2.f0.getId());
                        String placeId = "";
                        if(stringBooleanTuple2.f0 instanceof CommentEvent) {
                            placeId = String.valueOf(((CommentEvent) stringBooleanTuple2.f0).getPlaceId());
                        } else if (stringBooleanTuple2.f0 instanceof PostEvent) {
                            placeId = String.valueOf(((PostEvent) stringBooleanTuple2.f0).getPlaceId());
                        }
                        String output = id + ", " + personId + ", " + creationDate + ", " + placeId;
                        return output;
                    }
                });

        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH:mm:ss");

        String fileName = ConfigLoader.getUnusualActivityPath();
        int iend = fileName.lastIndexOf(".");
        String saveFilePath = fileName.substring(0 , iend) + "-" + formatter.format(date) + fileName.substring(iend);
        String csvHeader = "Id, PersonId, CreationDate, PlaceId";

        allFrauds.writeUsingOutputFormat(new WriteOutputFormat(saveFilePath, csvHeader)).setParallelism(1);
    }
}
