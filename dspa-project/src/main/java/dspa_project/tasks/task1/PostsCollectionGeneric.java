package dspa_project.tasks.task1;

import java.util.HashMap;
import java.util.Map;


public class PostsCollectionGeneric extends HashMap<Long, EventsCollection> {
    public PostsCollectionGeneric(){
        super();
    }
    public PostsCollectionGeneric(int initialCapacity) {
        super(initialCapacity);
    }
    public PostsCollectionGeneric(int initialCapacity, float loadFactor){
        super(initialCapacity,loadFactor);
    }
    public PostsCollectionGeneric(Map<? extends Long,? extends EventsCollection> m){
        super(m);
    }
}