package dspa_project.tasks.task3;

import dspa_project.config.ConfigLoader;
import dspa_project.model.CommentEvent;
import dspa_project.model.PostEvent;
import dspa_project.stream.sources.SimulationSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Task3 {

    public Task3( StreamExecutionEnvironment env ) {
        SourceFunction<CommentEvent> sourceFraudComments = new SimulationSourceFunction<CommentEvent>("Task3","comment-topic", "dspa_project.schemas.CommentSchema",
                2, 10000, 10000);

        SourceFunction<PostEvent> sourceFraudPosts = new SimulationSourceFunction<PostEvent>("Task3","post-topic", "dspa_project.schemas.PostSchema",
                2, 10000, 10000);

        TypeInformation<CommentEvent> typeInfoComments = TypeInformation.of(CommentEvent.class);
        TypeInformation<PostEvent> typeInfoPosts = TypeInformation.of(PostEvent.class);

        DataStream<Tuple2<CommentEvent, Boolean>> fraudComments = env.addSource(sourceFraudComments, typeInfoComments)
                .map(new MapFunction<CommentEvent, Tuple2<CommentEvent, Boolean>>() {
                    @Override
                    public Tuple2<CommentEvent, Boolean> map(CommentEvent commentEvent) throws Exception {
                        boolean fraud = !UnusualActivityDetection.checkLocation(commentEvent.getPersonId(), commentEvent.getPlaceId());
                        return new Tuple2<>(commentEvent, fraud);
                    }
                })
                .filter(new FilterFunction<Tuple2<CommentEvent, Boolean>>() {
                    @Override
                    public boolean filter(Tuple2<CommentEvent, Boolean> commentEventTuple2) throws Exception {
                        return commentEventTuple2.f1;
                    }
                });

        fraudComments.print();

        DataStream<Tuple2<PostEvent, Boolean>> fraudPosts = env.addSource(sourceFraudPosts, typeInfoPosts)
                .map(new MapFunction<PostEvent, Tuple2<PostEvent, Boolean>>() {
                    @Override
                    public Tuple2<PostEvent, Boolean> map(PostEvent postEvent) throws Exception {
                        boolean fraud = !UnusualActivityDetection.checkLocation(postEvent.getPersonId(), postEvent.getPlaceId());
                        return new Tuple2<>(postEvent, fraud);
                    }
                })
                .filter(new FilterFunction<Tuple2<PostEvent, Boolean>>() {
                    @Override
                    public boolean filter(Tuple2<PostEvent, Boolean> posttEventTuple2) throws Exception {
                        return posttEventTuple2.f1;
                    }
                });

        fraudPosts.print();

        // TODO: update config so that it gets different names for the streams
        //fraudComments.writeAsCsv(ConfigLoader.getUnusualActivityPath());
        //fraudPosts.writeAsCsv(ConfigLoader.getUnusualActivityPath());
    }
}
