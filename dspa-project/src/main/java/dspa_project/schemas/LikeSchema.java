package dspa_project.schemas;

import dspa_project.model.LikeEvent;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Date;
import java.util.Map;

public class LikeSchema extends AbstractDeserializationSchema<Tuple2<Long, LikeEvent>> implements Serializer<LikeEvent> {        //, Deserializer<LikeEvent>
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        this.isKey = b;
    }

    @Override
    public byte[] serialize(String s, LikeEvent likeEvent) {
        byte[] bytes = null;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(stream);

            out.writeLong(likeEvent.getId());
            out.writeLong(likeEvent.getPersonId());
            out.writeLong(likeEvent.getCreationDate().getTime());
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
    public Tuple2<Long, LikeEvent> deserialize(byte[] bytes) {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        DataInputStream in;
        LikeEvent event = null;



        try {
            in = new DataInputStream(stream);

            long id = in.readLong();
            long personId = in.readLong();
            Date date = new Date(in.readLong());

            event = new LikeEvent(id, personId, date);

            System.out.println("Deserialize!!!" + id);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new Tuple2<>(event.getId(), event);
    }
}
