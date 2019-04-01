package dspa_project.model;

import java.io.*;
import java.util.Date;

public class LikeEvent extends EventInterface {

    public LikeEvent(String [] data){
        super(data);
    }

    public LikeEvent(long postID, long personID, Date date){
        super(postID, personID, date);
    }

    @Override
    public byte[] serialize() {
        byte[] bytes = null;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        DataOutputStream out = null;
        try {
            out = new DataOutputStream( stream );

            out.writeLong( this.getId() );
            out.writeLong( this.getPersonId() );
            out.writeLong( this.getCreationDate().getTime() );
            out.flush();

            // Return the serialized object.
            bytes = stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bytes;
    }

    public static EventInterface deserialize(byte[] bytes) {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        DataInputStream in = null;
        LikeEvent event = null;

        try {
            in = new DataInputStream(stream);

            long id = in.readLong();
            long personId = in.readLong();
            Date date = new Date(in.readLong());

            event = new LikeEvent(id, personId, date);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return event;
    }
}
