package dspa_project.stream.sources;

import dspa_project.DataLoader;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

import static dspa_project.stream.sources.SimulationSourceFunction.LOCAL_KAFKA_BROKER;
import static java.lang.System.*;
import static sun.misc.MessageUtils.err;

public class KafkaCreator {
    private Properties props = new Properties();
    private DataLoader dataLoader;
    private static String LOCAL_KAFKA_BROKER = "localhost:9092";

    public KafkaCreator() throws IOException {
        dataLoader = new DataLoader();

        props.put("bootstrap.servers", LOCAL_KAFKA_BROKER);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void startLikeStream() throws IOException {
        int i = 0;

        try {
            props.put("value.serializer", Class.forName("dspa_project.schemas.LikeSchema"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Producer<String, LikeEvent> likeProducer = new KafkaProducer<>(props);
		LikeEvent likeEvent = dataLoader.parseLike();
		while (likeEvent != null) {
			if(i%10000 == 0)
				out.println("Like: " + i);
			i++;
			likeProducer.send(new ProducerRecord<>("like-topic", likeEvent));
			likeEvent = dataLoader.parseLike();
        }

		likeProducer.close();
    }

    public void startCommentStream() throws IOException {
        int i = 0;

        try {
            props.put("value.serializer", Class.forName("dspa_project.schemas.CommentSchema"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Producer<String, CommentEvent> commentProducer = new KafkaProducer<>(props);
        CommentEvent commentEvent = dataLoader.parseComment();
        while (commentEvent != null) {
            if(i%10000 == 0)
                System.out.println("Comment: " + i);
            i++;
            commentProducer.send(new ProducerRecord<>("comment-topic", commentEvent));
            commentEvent = dataLoader.parseComment();
        }

        commentProducer.close();
    }

    public void startPostStream() throws IOException {

        try {
            props.put("value.serializer", Class.forName("dspa_project.schemas.PostSchema"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Producer<String, PostEvent> postProducer = new KafkaProducer<>(props);
		PostEvent postEvent = dataLoader.parsePost();
		int i = 0;
		while (postEvent != null) {
			if(i%10000 == 0)
				System.out.println("Post: " + i);
			i++;
			postProducer.send(new ProducerRecord<>("post-topic", postEvent));
			postEvent = dataLoader.parsePost();
		}

		postProducer.close();
    }
}
