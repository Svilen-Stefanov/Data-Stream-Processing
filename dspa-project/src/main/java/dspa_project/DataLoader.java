package dspa_project;

import dspa_project.config.ConfigLoader;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataLoader {

    private BufferedReader commentBr;
    private BufferedReader likesBr;
    private BufferedReader postsBr;

    public DataLoader() throws IOException {
        ConfigLoader.load();
        commentBr = new BufferedReader(new FileReader(ConfigLoader.getCommentEvent()));
        likesBr = new BufferedReader(new FileReader(ConfigLoader.getLikeEvent()));
        postsBr = new BufferedReader(new FileReader(ConfigLoader.getPostEvent()));

        // skip headers
        commentBr.readLine();
        likesBr.readLine();
        postsBr.readLine();
    }

    public CommentEvent parseComment() throws IOException {
        String line = commentBr.readLine();

        if(line == null)
            return null;

        String [] data = line.split("\\|");
        CommentEvent commentEvent = new CommentEvent(data);
        return commentEvent;
    }

    public LikeEvent parseLike() throws IOException {
        String line = likesBr.readLine();

        if(line == null)
            return null;

        String [] data = line.split("\\|");
        LikeEvent likeEvent = new LikeEvent(data);
        return likeEvent;
    }

    public PostEvent parsePost() throws IOException {
        String line = postsBr.readLine();

        if(line == null)
            return null;

        String [] data = line.split("\\|");
        PostEvent postEvent = new PostEvent(data);
        return postEvent;
    }
}
