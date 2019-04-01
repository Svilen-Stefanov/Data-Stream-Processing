package dspa_project.schemas;

import dspa_project.model.PostEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Date;
import java.util.Map;

public class PostSchema implements Serializer<PostEvent>, Deserializer<PostEvent> {
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        this.isKey = b;
    }

    @Override
    public byte[] serialize(String s, PostEvent postEvent) {
        byte[] bytes = null;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream out = null;
        try {
            out = new DataOutputStream( stream );

            out.writeLong( postEvent.getId() );
            out.writeLong( postEvent.getPersonId() );
            out.writeLong( postEvent.getCreationDate().getTime() );
            out.writeUTF( postEvent.getLanguage());
            out.writeUTF( postEvent.getContent() );
            int numberOfTags = postEvent.getTag().length;
            out.writeInt( numberOfTags );
            for (int i = 0; i < numberOfTags; i++){
                out.writeLong( postEvent.getTag()[i] );
            }
            out.writeLong( postEvent.getForumId() );
            out.writeLong( postEvent.getPlaceId() );

            out.flush();

            // Return the serialized object.
            bytes = stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bytes;
    }

    @Override
    public PostEvent deserialize(String s, byte[] bytes) {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        DataInputStream in = null;
        PostEvent event = null;

        try {
            in = new DataInputStream(stream);

            long id = in.readLong();
            long personId = in.readLong();
            Date creationDate = new Date(in.readLong());
            String language = in.readUTF();
            String content = in.readUTF();
            int numberOfTags = in.readInt();
            long [] tagIds = new long[numberOfTags];
            for (int i = 0; i < numberOfTags; i++) {
                tagIds[i] = in.readLong();
            }
            long forumId = in.readLong();
            long placeId = in.readLong();

            event = new PostEvent(id, personId, creationDate, language, content, tagIds, forumId, placeId);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return event;
    }

    @Override
    public void close() {

    }
}

