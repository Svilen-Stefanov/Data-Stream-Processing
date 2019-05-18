package dspa_project.config;

import dspa_project.database.init.MySQLJDBCUtil;
import dspa_project.database.queries.SQLQuery;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.Person;
import dspa_project.model.PostEvent;
import me.tongfei.progressbar.ProgressBar;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataLoader {

    // Streaming buffers
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
        String tmp = data[1];
        data[1] = data[0];
        data[0] = tmp;
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

    /* STATIC DATA */
    public void parseStaticData(){
        try {
            ProgressBar pb = new ProgressBar("Creating static database", 12);
            pb.start();

            parsePersonsInterests();
            pb.step();

            parseTags();
            pb.step();

            parseTagClasses();
            pb.step();

            parseTagTypes();
            pb.step();

            parseTagIsSubclasses();
            pb.step();

            parsePersonKnowsPerson();
            pb.step();

            parsePeople();
            pb.step();

            parseStudyAt();
            pb.step();

            parseWorkAt();
            pb.step();

            parseLanguage();
            pb.step();

            parseLocation();
            pb.step();

            parseParentPlace();
            pb.step();

            pb.stop();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO: maybe don't parse all people and keep them in memory
    // TODO: maybe make it even more generic by adding all these properties to the config file
    private void parsePeople() throws IOException {
        BufferedReader personBr = new BufferedReader(new FileReader(ConfigLoader.getPersonPath()));
        personBr.readLine();
        String PERSON_TABLE = "person";

        String [] attributeNames = {"ID", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATION_DATE", "LOCATION_IP", "BROWSER_USED"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(50)", "DATETIME", "DATETIME", "VARCHAR(50)", "VARCHAR(50)"};
        SQLQuery.createAndFillTable(PERSON_TABLE, attributeNames, attributeTypes, personBr);
    }

    private void parseWorkAt() throws IOException {
        BufferedReader workAtBr = new BufferedReader(new FileReader(ConfigLoader.getWorkAtPath()));
        workAtBr.readLine();
        String WORK_AT_TABLE = "work_at";

        String [] attributeNames = {"PERSON_ID", "ORGANIZATION_ID", "WORK_FROM"};
        String [] attributeTypes = {"BIGINT", "BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(WORK_AT_TABLE, attributeNames, attributeTypes, workAtBr);
    }

    private void parseStudyAt() throws IOException {
        BufferedReader studyAtBr = new BufferedReader(new FileReader(ConfigLoader.getStudyAtPath()));
        studyAtBr.readLine();
        String STUDY_AT_TABLE = "study_at";

        String [] attributeNames = {"PERSON_ID", "ORGANIZATION_ID", "CLASS_YEAR"};
        String [] attributeTypes = {"BIGINT", "BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(STUDY_AT_TABLE, attributeNames, attributeTypes, studyAtBr);
    }

    private void parseLanguage() throws IOException {
        BufferedReader lanuageBr = new BufferedReader(new FileReader(ConfigLoader.getLanguagePath()));
        lanuageBr.readLine();
        String SPEAKS_LANGUAGE_TABLE = "speaks_language";

        String [] attributeNames = {"PERSON_ID", "LANGUAGE"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(50)"};
        SQLQuery.createAndFillTable(SPEAKS_LANGUAGE_TABLE, attributeNames, attributeTypes, lanuageBr);
    }

    private void parseLocation() throws IOException {
        BufferedReader locationBr = new BufferedReader(new FileReader(ConfigLoader.getLocationPath()));
        locationBr.readLine();
        String LOCATION_TABLE = "person_isLocatedIn_place";

        String [] attributeNames = {"PERSON_ID", "PLACE_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(LOCATION_TABLE, attributeNames, attributeTypes, locationBr);
    }

    private void parseParentPlace() throws IOException {
        BufferedReader parentPlaceBr = new BufferedReader(new FileReader(ConfigLoader.getParentPlace()));
        parentPlaceBr.readLine();
        String PLACE_PARENT_TABLE = "place_isPartOf_place";

        String [] attributeNames = {"PLACE_ID_A", "PLACE_ID_B"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(PLACE_PARENT_TABLE, attributeNames, attributeTypes, parentPlaceBr);
        SQLQuery.updateEngladParentLocation();
    }

    private void parsePersonsInterests() throws IOException {
        BufferedReader personsInterestsBr = new BufferedReader(new FileReader(ConfigLoader.getPersonsInterestsPath()));
        personsInterestsBr.readLine();
        String PERSON_INTEREST_TABLE = "person_hasInterest_tag";

        String [] attributeNames = {"PERSON_ID", "TAG_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(PERSON_INTEREST_TABLE, attributeNames, attributeTypes, personsInterestsBr);
    }

    private void parseTags() throws IOException {
        BufferedReader tagBr = new BufferedReader(new FileReader(ConfigLoader.getTagPath()));
        tagBr.readLine();
        String TAG_TABLE = "tag";

        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(100)", "VARCHAR(255)"};
        SQLQuery.createAndFillTable(TAG_TABLE, attributeNames, attributeTypes, tagBr);
    }

    private void parseTagClasses() throws IOException {
        BufferedReader tagClassBr = new BufferedReader(new FileReader(ConfigLoader.getTagClassPath()));
        tagClassBr.readLine();
        String TAG_CLASS_TABLE = "tagclass";

        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(100)", "VARCHAR(255)"};
        SQLQuery.createAndFillTable(TAG_CLASS_TABLE, attributeNames, attributeTypes, tagClassBr);
    }

    private void parseTagTypes() throws IOException {
        BufferedReader tagTypeBr = new BufferedReader(new FileReader(ConfigLoader.getTagTypePath()));
        tagTypeBr.readLine();
        String TAG_TYPE_TABLE = "tag_hasType_tagclass";

        String [] attributeNames = {"TAG_ID", "TAG_CLASS_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(TAG_TYPE_TABLE, attributeNames, attributeTypes, tagTypeBr);
    }

    private void parseTagIsSubclasses() throws IOException {
        BufferedReader tagIsSubclassBr = new BufferedReader(new FileReader(ConfigLoader.getTagIsSubclassPath()));
        tagIsSubclassBr.readLine();
        String TAG_IS_SUBCLASS_TABLE = "tagclass_isSubclassOf_tagclass";

        String [] attributeNames = {"TAG_CLASS_ID", "PARENT_TAG_CLASS_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(TAG_IS_SUBCLASS_TABLE, attributeNames, attributeTypes, tagIsSubclassBr);
    }

    private void parsePersonKnowsPerson() throws IOException {
        BufferedReader personKnowsPersonBr = new BufferedReader(new FileReader(ConfigLoader.getPersonKnowsPersonPath()));
        personKnowsPersonBr.readLine();
        String PERSON_KNOWS_PERSON_TABLE = "person_knows_person";

        String [] attributeNames = {"PERSON_ID_A", "PERSON_ID_B"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        SQLQuery.createAndFillTable(PERSON_KNOWS_PERSON_TABLE, attributeNames, attributeTypes, personKnowsPersonBr);
    }
}
