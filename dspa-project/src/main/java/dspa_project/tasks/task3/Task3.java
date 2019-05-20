package dspa_project.tasks.task3;

import dspa_project.config.ConfigLoader;
import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;
import dspa_project.model.PostEvent;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;

public class Task3 {

    public Task3( StreamExecutionEnvironment env ) {
        SourceFunction<CommentEvent> sourceFraudComments = new SimulationSourceFunction<CommentEvent>("Task3","comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);

        SourceFunction<PostEvent> sourceFraudPosts = new SimulationSourceFunction<PostEvent>("Task3","post-topic", "dspa_project.schemas.PostSchema",
                2, 10000, 10000);

        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);

        DataStream<Tuple2<String, Boolean>> fraudComments = env.addSource(sourceFraudComments, typeInfoComments)
                .map(new MapFunction<CommentEvent, Tuple2<String, Boolean>>() {
                    @Override
                    public Tuple2<String, Boolean> map(CommentEvent commentEvent) throws Exception {
                        boolean fraud = UnusualActivityDetection.checkFraud(commentEvent.getPersonId(), commentEvent.getPlaceId());
                        return new Tuple2<>(commentEvent.toString(), fraud);
                    }
                })
                .filter(new FilterFunction<Tuple2<String, Boolean>>() {
                    @Override
                    public boolean filter(Tuple2<String, Boolean> commentEventTuple2) throws Exception {
                        return commentEventTuple2.f1;
                    }
                });

        DataStream<Tuple2<String, Boolean>> fraudPosts = env.addSource(sourceFraudPosts, typeInfoPosts)
                .map(new MapFunction<PostEvent, Tuple2<String, Boolean>>() {
                    @Override
                    public Tuple2<String, Boolean> map(PostEvent postEvent) throws Exception {
                        boolean fraud = UnusualActivityDetection.checkFraud(postEvent.getPersonId(), postEvent.getPlaceId());
                        return new Tuple2<>(postEvent.toString(), fraud);
                    }
                })
                .filter(new FilterFunction<Tuple2<String, Boolean>>() {
                    @Override
                    public boolean filter(Tuple2<String, Boolean> posttEventTuple2) throws Exception {
                        return posttEventTuple2.f1;
                    }
                });

        DataStream<Tuple2<String, Boolean>> allFrauds = fraudComments.union(fraudPosts);

        String saveFilePath = ConfigLoader.getUnusualActivityPath();

        File tmpDir = new File(saveFilePath);
        boolean fileExists = tmpDir.exists();

        if (!fileExists) {
            allFrauds.writeAsText(saveFilePath).setParallelism(1);
        } else {
            String outputMsg = "----------------------------\n" +
                    "----------------------------\n" +
                    "Output file already exists!\n" +
                    "Delete the file or change the file name if you want to store it again!\n" +
                    "----------------------------\n" +
                    "----------------------------";
            System.out.println(outputMsg);
            System.exit(1);
        }
    }
}
