package dspa_project.model;

import java.util.Date;

public class CommentEvent extends EventInterface {
    private String content;
    private long replyToPostId;
    private long replyToCommentId;
    private long placeId;

    public CommentEvent(long id, long personId, Date creationDate, String content, long replyToPostId, long replyToCommentId, long placeId) {
        super(id, personId, creationDate);
        this.content = content;
        this.replyToPostId = replyToPostId;
        this.replyToCommentId = replyToCommentId;
        this.placeId = placeId;
    }

    public CommentEvent(String[] data) {
        super(data);
        this.content = data[5];
        this.replyToPostId = parseId(data[6]);
        this.replyToCommentId = parseId(data[7]);
        this.placeId = parseId(data[8]);
    }

    @Override
    public String toString() {
        String short_content = this.getContent().length() < 100 ? this.getContent() : this.getContent().substring(0,99) + "...";
        short_content = "\"" + short_content + "\"";
        String output = super.toString() +
                ", Content:" + short_content +
                ", ReplyToPostID:" + this.getReplyToPostId() +
                ", ReplyToCommentID:" + this.getReplyToCommentId() +
                ", PlaceID:" + this.getPlaceId();
        return output;
    }

    public String getContent() {
        return content;
    }

    public long getReplyToPostId() {
        return replyToPostId;
    }

    public long getReplyToCommentId() {
        return replyToCommentId;
    }

    public long getPlaceId() {
        return placeId;
    }

    @Override
    public long getPostId() {
        return getReplyToPostId();
    }
}
