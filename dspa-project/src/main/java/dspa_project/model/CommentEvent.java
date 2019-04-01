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

    public byte[] serialize(CommentEvent event) { return null; }

    public static CommentEvent deserialize(byte[] bytes) { return null; }
}
