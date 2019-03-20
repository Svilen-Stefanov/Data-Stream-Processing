package dspa_project.event;

import org.apache.commons.lang3.ObjectUtils;

import java.util.Date;

public class PostEvent extends EventInterface {
    private String language;
    private String content;
    private long [] tagIds;
    private long forumId;
    private long placeId;

    public PostEvent(String[] data) {
        super(data);
        this.language = data[6];
        this.content = data[7];

        // parse tagIds
        int idx = 0;
        String[] tags;
        if(data[8].equals("")){
            tags = new String[0];
        } else {
            tags = data[8].replace("[", "").replace("]", "").split(",");
        }

        this.tagIds = new long[tags.length];
        for (String s : tags){
            tagIds[idx++] = Long.parseLong(s.trim());
        }

        this.forumId = Long.parseLong(data[9]);
        this.placeId = parseId(data[10]);
    }

    public PostEvent(long id, long personId, Date creationDate, String language, String content, long [] tagIds, long forumId, long placeId) {
        super(id, personId, creationDate);
        this.language = language;
        this.content = content;
        //this.tag = tag;
        this.forumId = forumId;
        this.placeId = placeId;
    }

    public String getLanguage() {
        return language;
    }

    public String getContent() {
        return content;
    }

    public long [] getTag() {
        return tagIds;
    }

    public long getForumId() {
        return forumId;
    }

    public long getPlaceId() {
        return placeId;
    }
}
