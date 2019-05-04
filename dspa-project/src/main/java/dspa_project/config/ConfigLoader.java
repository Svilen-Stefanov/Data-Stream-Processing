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

    private static final String PERSON = "person";
    private static final String WORK_AT = "person_workAt_organisation";
    private static final String STUDY_AT = "person_studyAt_organisation";
    private static final String SPEAKS_LANGUAGE = "person_speaks_language";
    private static final String LOCATED_IN = "person_isLocatedIn_place";
    private static final String PLACE_IS_IN_PLACE = "place_isPartOf_place";

    private static final String PERSONS_INTERESTS = "persons_interests";
    private static final String TAG = "tag";
    private static final String TAGCLASS = "tagclass";
    private static final String TAG_TYPE = "tag_hasType";
    private static final String TAG_ISSUBCLASS = "tagclass_isSubclass";
    private static final String PERSON_KNOWS_PERSON = "person_knows_person";

    private static String sql_user;
    private static String sql_password;
    private static String sql_url;

    private static boolean loaded = false;

    private static String likesPath, commentEventsPath, postEventsPath;
    private static String personPath, workAtPath, studyAtPath, speaksLanguagePath, locatedInPlacePath, placeIsInPlacePath;
    private static String personsInterestsPath, tagPath, tagClassPath, tagTypePath, tagIsSubclassPath, personKnowsPersonPath;

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

            // get mysql setup data
            sql_user = (doc.getElementsByTagName("user").item(0)).getFirstChild().getTextContent();
            sql_password = (doc.getElementsByTagName("password").item(0)).getFirstChild().getTextContent();
            sql_url = (doc.getElementsByTagName("url").item(0)).getFirstChild().getTextContent();

            NodeList nList = doc.getElementsByTagName("path");

            for (int temp = 0; temp < nList.getLength(); temp++) {

                Node nNode = nList.item(temp);

                if (nNode.getNodeType() == Node.ELEMENT_NODE) {

                    Element eElement = (Element) nNode;

                    switch (eElement.getAttribute("name")) {
                        // Streaming data
                        case COMMENT_EVENT:
                            commentEventsPath = eElement.getFirstChild().getTextContent();
                            break;
                        case LIKE_EVENT:
                            likesPath = eElement.getFirstChild().getTextContent();
                            break;
                        case POST_EVENT:
                            postEventsPath = eElement.getFirstChild().getTextContent();
                            break;

                        // Static data
                        case PERSON:
                            personPath = eElement.getFirstChild().getTextContent();
                            break;
                        case STUDY_AT:
                            studyAtPath = eElement.getFirstChild().getTextContent();
                            break;
                        case WORK_AT:
                            workAtPath = eElement.getFirstChild().getTextContent();
                            break;
                        case SPEAKS_LANGUAGE:
                            speaksLanguagePath = eElement.getFirstChild().getTextContent();
                            break;
                        case LOCATED_IN:
                            locatedInPlacePath = eElement.getFirstChild().getTextContent();
                            break;
                        case PLACE_IS_IN_PLACE:
                            placeIsInPlacePath = eElement.getFirstChild().getTextContent();
                            break;

                        case PERSONS_INTERESTS:
                            personsInterestsPath = eElement.getFirstChild().getTextContent();
                            break;
                        case TAG:
                            tagPath = eElement.getFirstChild().getTextContent();
                            break;
                        case TAGCLASS:
                            tagClassPath = eElement.getFirstChild().getTextContent();
                            break;
                        case TAG_TYPE:
                            tagTypePath = eElement.getFirstChild().getTextContent();
                            break;
                        case TAG_ISSUBCLASS:
                            tagIsSubclassPath = eElement.getFirstChild().getTextContent();
                            break;
                        case PERSON_KNOWS_PERSON:
                            personKnowsPersonPath = eElement.getFirstChild().getTextContent();
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

    public static String getPersonsInterestsPath() {
        return personsInterestsPath;
    }

    public static String getTagPath() {
        return tagPath;
    }

    public static String getTagClassPath() {
        return tagClassPath;
    }

    public static String getTagTypePath() {
        return tagTypePath;
    }

    public static String getTagIsSubclassPath() {
        return tagIsSubclassPath;
    }

    public static String getPersonKnowsPersonPath() {
        return personKnowsPersonPath;
    }

    public static String getPersonPath() { return personPath; }

    public static String getWorkAtPath() {
        return workAtPath;
    }

    public static String getStudyAtPath() {
        return studyAtPath;
    }

    public static String getLanguagePath() {
        return speaksLanguagePath;
    }

    public static String getLocationPath() {
        return locatedInPlacePath;
    }

    public static String getParentPlace() {
        return placeIsInPlacePath;
    }

    public static String getSql_user() {
        return sql_user;
    }

    public static String getSql_password() {
        return sql_password;
    }

    public static String getSql_url() {
        return sql_url;
    }
}
