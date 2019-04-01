package dspa_project.model;

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

    @Override
    public String toString() {
        String short_content = this.getContent().length() < 100 ? this.getContent() : this.getContent().substring(0,99) + "...";
        short_content = "\"" + short_content + "\"";
        String output = super.toString() +
                ", Language:" + this.getLanguage() +
                ", Content:" + short_content +
                ", ForumID:" + this.getForumId() +
                ", PlaceID:" + this.getPlaceId();
        output += ", TagIDs:[";
        long[] tags = getTags();
        if ( tags == null ) {
            tags = new long[0];
        }
        for ( long tag : tags ) {
            output += tag + " ";
        }
        output += "]";
        return output;
    }

    public String getLanguage() {
        return language;
    }

    public String getContent() {
        return content;
    }

    public long [] getTags() {
        return tagIds;
    }

    public long getForumId() {
        return forumId;
    }

    public long getPlaceId() {
        return placeId;
    }
}
