package dspa_project.stream.sources;

import dspa_project.config.ConfigLoader;
import dspa_project.config.DataLoader;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;
import me.tongfei.progressbar.ProgressBar;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class KafkaCreator {
    private Properties props = new Properties();
    private DataLoader dataLoader;
    private static String LOCAL_KAFKA_BROKER = "localhost:9092";
    private long numberOfLikes;
    private long numberOfComments;
    private long numberOfPosts;

    public KafkaCreator() throws IOException {
        dataLoader = new DataLoader();


        ProgressBar pb = new ProgressBar("Check data size", 3);
        pb.start();
        Path path;
        path = Paths.get(ConfigLoader.getLikeEvent());
        numberOfLikes = Files.lines(path).count();
        pb.step();
        path = Paths.get(ConfigLoader.getCommentEvent());
        numberOfComments = Files.lines(path).count();
        pb.step();
        path = Paths.get(ConfigLoader.getPostEvent());
        numberOfPosts = Files.lines(path).count();
        pb.step();
        pb.stop();

        props.put("bootstrap.servers", LOCAL_KAFKA_BROKER);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void startLikeStream( long count ) throws IOException, ClassNotFoundException {
        props.put("value.serializer", Class.forName("dspa_project.schemas.LikeSchema"));

        Producer<String, LikeEvent> likeProducer = new KafkaProducer<>(props);
		LikeEvent likeEvent = dataLoader.parseLike();
		// 662 891 - 1k
		// 21 148 772 - 10k
        numberOfLikes = (count > 0 && count < numberOfLikes) ? count : numberOfLikes;
        ProgressBar pb = new ProgressBar("Generating like stream", numberOfLikes);
        pb.start();
        pb.step();
		while (likeEvent != null) {
			pb.step();
			if ( count >= 0 && pb.getCurrent() == count ) {
			    break;
            }
			likeProducer.send(new ProducerRecord<>("like-topic", likeEvent));
			likeEvent = dataLoader.parseLike();
        }
		pb.stop();

		likeProducer.close();
    }

    public void startCommentStream( long count ) throws IOException, ClassNotFoundException {
        props.put("value.serializer", Class.forName("dspa_project.schemas.CommentSchema"));

        Producer<String, CommentEvent> commentProducer = new KafkaProducer<>(props);
        CommentEvent commentEvent = dataLoader.parseComment();
        // 632 043 - 1k
        // 20 096 289 - 10k
        numberOfComments = (count > 0 && count < numberOfComments) ? count : numberOfComments;
        ProgressBar pb = new ProgressBar("Generating comment stream", numberOfComments);
        pb.start();
        pb.step();
        while (commentEvent != null) {
            pb.step();
            if ( count >= 0 && pb.getCurrent() == count ) {
                break;
            }
            commentProducer.send(new ProducerRecord<>("comment-topic", commentEvent));
            commentEvent = dataLoader.parseComment();
        }
        pb.stop();

        commentProducer.close();
    }

    public void startPostStream( long count ) throws IOException, ClassNotFoundException {

        props.put("value.serializer", Class.forName("dspa_project.schemas.PostSchema"));

        Producer<String, PostEvent> postProducer = new KafkaProducer<>(props);
		PostEvent postEvent = dataLoader.parsePost();
		// 173 402 - 1k
        // 5 520 843 - 10k
        numberOfPosts = (count > 0 && count < numberOfPosts) ? count : numberOfPosts;
        ProgressBar pb = new ProgressBar("Generating post stream", numberOfPosts);
        pb.start();
        pb.step();
		while (postEvent != null) {
            pb.step();
            if ( count >= 0 && pb.getCurrent() == count ) {
                break;
            }
			postProducer.send(new ProducerRecord<>("post-topic", postEvent));
			postEvent = dataLoader.parsePost();
		}
		pb.stop();

		postProducer.close();
    }
}
