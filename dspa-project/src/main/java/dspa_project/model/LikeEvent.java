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

}
