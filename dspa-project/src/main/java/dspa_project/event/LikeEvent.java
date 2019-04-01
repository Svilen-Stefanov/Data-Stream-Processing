package dspa_project.event;

import java.util.Date;

public class LikeEvent extends EventInterface {

    public LikeEvent(String [] data){
        super(data);
    }

    public LikeEvent(long postID, long personID, Date date){
        super(postID, personID, date);
    }

    public byte[] serialize(LikeEvent event) { return null; }

    public static LikeEvent deserialize(byte[] bytes) { return null; }
}
