package dspa_project.tasks.task1;

import java.util.HashMap;
import java.util.Map;


public class PostsCollection extends HashMap<Long, CommentsCollection> {
    public PostsCollection(){
        super();
    }
    public PostsCollection(int initialCapacity) {
        super(initialCapacity);
    }
    public PostsCollection(int initialCapacity, float loadFactor){
        super(initialCapacity,loadFactor);
    }
    public PostsCollection(Map<? extends Long,? extends CommentsCollection> m){
        super(m);
    }
}