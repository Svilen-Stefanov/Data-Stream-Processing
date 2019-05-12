package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;

import java.util.ArrayList;
import java.util.Collection;

public class CommentsCollection extends ArrayList<CommentEvent> {
    public CommentsCollection(){
        super();
    }
    public CommentsCollection(Collection<? extends CommentEvent> c){
        super(c);
    }
    public CommentsCollection(int initialCapacity){
        super(initialCapacity);
    }
}