package dspa_project.config;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

public class ConfigLoader {

    private static final String configFilePath = "config.xml";
    private static final String COMMENT_EVENT = "comment_event_csv";
    private static final String LIKE_EVENT = "likes_csv";
    private static final String POST_EVENT = "post_event_csv";
    private static boolean loaded = false;

    private static String likesPath, commentEventsPath, postEventsPath;

    public static void load() {
        if(loaded)
            return;

        try {
            File fXmlFile = new File(configFilePath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);

            //optional, but recommended
            //read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            NodeList nList = doc.getElementsByTagName("path");

            for (int temp = 0; temp < nList.getLength(); temp++) {

                Node nNode = nList.item(temp);

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                    Element eElement = (Element) nNode;

                    switch (eElement.getAttribute("name")) {
                        case COMMENT_EVENT:
                            commentEventsPath = eElement.getFirstChild().getTextContent();
                            break;
                        case LIKE_EVENT:
                            likesPath = eElement.getFirstChild().getTextContent();
                            break;
                        case POST_EVENT:
                            postEventsPath = eElement.getFirstChild().getTextContent();
                            break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        loaded = true;
    }

    public static String getCommentEvent() {
        return commentEventsPath;
    }

    public static String getLikeEvent() {
        return likesPath;
    }

    public static String getPostEvent() {
        return postEventsPath;
    }
}
