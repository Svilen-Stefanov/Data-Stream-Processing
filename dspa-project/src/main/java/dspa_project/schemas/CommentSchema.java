package dspa_project.schemas;

import dspa_project.model.CommentEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Date;
import java.util.Map;

public class CommentSchema implements Serializer<CommentEvent>, Deserializer<CommentEvent> {      //Deserializer<CommentEvent>
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        this.isKey = b;
    }

    @Override
    public byte[] serialize(String s, CommentEvent commentEvent) {
        byte[] bytes = null;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream out = null;
        try {
            out = new DataOutputStream( stream );

            out.writeLong( commentEvent.getId() );
            out.writeLong( commentEvent.getPersonId() );
            out.writeLong( commentEvent.getCreationDate().getTime() );
            out.writeUTF( commentEvent.getContent() );
            out.writeLong( commentEvent.getReplyToPostId());
            out.writeLong( commentEvent.getReplyToCommentId() );
            out.writeLong( commentEvent.getPlaceId() );

            out.flush();

            // Return the serialized object.
            bytes = stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bytes;
    }

    @Override
    public void close() {

    }

    @Override
    public CommentEvent deserialize( String topic, byte[] bytes ) {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        DataInputStream in;
        CommentEvent event = null;

        try {
            in = new DataInputStream(stream);

            long id = in.readLong();
            long personId = in.readLong();
            Date date = new Date(in.readLong());
            String content = in.readUTF();
            long getReplyToPostId = in.readLong();
            long replyToCommentId = in.readLong();
            long placeId = in.readLong();

            event = new CommentEvent(id, personId, date, content, getReplyToPostId, replyToCommentId, placeId);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return event;
    }
}

