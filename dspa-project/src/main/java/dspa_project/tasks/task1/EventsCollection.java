package dspa_project.tasks.task1;

import dspa_project.model.CommentEvent;
import dspa_project.model.EventInterface;

import java.util.ArrayList;
import java.util.Collection;

public class EventsCollection extends ArrayList<EventInterface> {
    public EventsCollection(){
        super();
    }
    public EventsCollection(Collection<? extends CommentEvent> c){
        super(c);
    }
    public EventsCollection(int initialCapacity){
        super(initialCapacity);
    }
}