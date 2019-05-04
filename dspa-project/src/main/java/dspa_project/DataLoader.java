package dspa_project;

import dspa_project.config.ConfigLoader;
import dspa_project.database.init.MySQLJDBCUtil;
import dspa_project.model.CommentEvent;
import dspa_project.model.LikeEvent;
import dspa_project.model.PostEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Math.max;

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

    private static void insertIntoMysqlTable(String [] attributeNames, String values, String tableName, Statement st) throws SQLException {
        String query = "INSERT INTO  `static_database`.`" + tableName + "` (";

        for (int i = 0; i < attributeNames.length; i++) {
            query += "`" + attributeNames[i] + "`" + " , ";
        }

        query = query.substring(0, query.length() - 3);
        query += ") ";
        values = values.substring(1);

        query += "VALUES " + values + ";";
        st.executeUpdate(query);
    }

    public static void resetTables(){
        Connection conn = null;
        Statement st = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            String query = "drop database static_database; create database static_database;";

            st.executeUpdate(query);
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(DataLoader.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } catch(Exception e){
            e.printStackTrace();
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(DataLoader.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

    public static void createAndFillTable(String tableName, String [] attributeNames, String [] attributeTypes, BufferedReader br){
        Connection conn = null;
        Statement st = null;
        try
        {
            conn = MySQLJDBCUtil.getConnection();
            st = conn.createStatement();

            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, tableName, null);

            if (!rs.next()) {
                String sqlCreate = "CREATE TABLE IF NOT EXISTS " + tableName + "(";

                for (int i = 0; i < attributeNames.length; i++) {
                    sqlCreate += attributeNames[i] + " " + attributeTypes[i] + ",";
                }

                sqlCreate = sqlCreate.substring(0, sqlCreate.length()-1);
                sqlCreate += ")";

                Statement stmt = conn.createStatement();
                stmt.execute(sqlCreate);

                String[] nextLine;
                String nextLineString;
                String values = "";

                int curBatchIndex = 0;
                int batch_size = 1000;
                while ((nextLineString = br.readLine()) != null) {
                    nextLine = nextLineString.split("\\|");

                    values += ",('";
                    for (int i = 0; i < nextLine.length; i++) {
                        String data = nextLine[i];
                        if (attributeNames[i] == "CREATION_DATE"){
                            // remove T for time
                            data = data.replace("T"," ");
                            // remove Z at the end of string
                            data = data.substring(0, data.length() - 1);
                        }
                        values += data + "','";
                    }
                    values = values.substring(0, values.length()-3);
                    values += "')";

                    if (curBatchIndex % batch_size == batch_size - 1) {
                        insertIntoMysqlTable(attributeNames, values, tableName, st);
                        values = "";
                    }

                    curBatchIndex++;
                }

                if (values.length() != 0)
                    insertIntoMysqlTable(attributeNames, values, tableName, st);
            }
        }
        catch (SQLException ex) {
            Logger lgr = Logger.getLogger(DataLoader.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } catch(Exception e){
            e.printStackTrace();
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    conn.close();
                }

            } catch (SQLException ex) {
                Logger lgr = Logger.getLogger(DataLoader.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

    /* STATIC DATA */
    public static void parseStaticData(){
        try {
            parsePersonsInterests();
            parseTags();
            parseTagClasses();
            parseTagTypes();
            parseTagIsSubclasses();
            parsePersonKnowsPerson();
            parsePeople();
            parseStudyAt();
            parseWorkAt();
            parseLanguage();
            parseLocation();
            parseParentPlace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //TODO: maybe don't parse all people and keep them in memory
    // TODO: maybe make it even more generic by adding all these properties to the config file
    public static void parsePeople() throws IOException {
        BufferedReader personBr = new BufferedReader(new FileReader(ConfigLoader.getPersonPath()));
        personBr.readLine();
        String PERSON_TABLE = "person";

        String [] attributeNames = {"ID", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATION_DATE", "LOCATION_IP", "BROWSER_USED"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(50)", "DATETIME", "DATETIME", "VARCHAR(50)", "VARCHAR(50)"};
        createAndFillTable(PERSON_TABLE, attributeNames, attributeTypes, personBr);
    }

    public static void parseWorkAt() throws IOException {
        BufferedReader workAtBr = new BufferedReader(new FileReader(ConfigLoader.getWorkAtPath()));
        workAtBr.readLine();
        String WORK_AT_TABLE = "work_at";

        String [] attributeNames = {"PERSON_ID", "ORGANIZATION_ID", "WORK_FROM"};
        String [] attributeTypes = {"BIGINT", "BIGINT", "BIGINT"};
        createAndFillTable(WORK_AT_TABLE, attributeNames, attributeTypes, workAtBr);
    }

    public static void parseStudyAt() throws IOException {
        BufferedReader studyAtBr = new BufferedReader(new FileReader(ConfigLoader.getStudyAtPath()));
        studyAtBr.readLine();
        String STUDY_AT_TABLE = "study_at";

        String [] attributeNames = {"PERSON_ID", "ORGANIZATION_ID", "CLASS_YEAR"};
        String [] attributeTypes = {"BIGINT", "BIGINT", "BIGINT"};
        createAndFillTable(STUDY_AT_TABLE, attributeNames, attributeTypes, studyAtBr);
    }

    public static void parseLanguage() throws IOException {
        BufferedReader lanuageBr = new BufferedReader(new FileReader(ConfigLoader.getLanguagePath()));
        lanuageBr.readLine();
        String SPEAKS_LANGUAGE_TABLE = "speaks_language";

        String [] attributeNames = {"PERSON_ID", "LANGUAGE"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(50)"};
        createAndFillTable(SPEAKS_LANGUAGE_TABLE, attributeNames, attributeTypes, lanuageBr);
    }

    public static void parseLocation() throws IOException {
        BufferedReader locationBr = new BufferedReader(new FileReader(ConfigLoader.getLocationPath()));
        locationBr.readLine();
        String LOCATION_TABLE = "person_isLocatedIn_place";

        String [] attributeNames = {"PERSON_ID", "PLACE_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(LOCATION_TABLE, attributeNames, attributeTypes, locationBr);
    }

    public static void parseParentPlace() throws IOException {
        BufferedReader parentPlaceBr = new BufferedReader(new FileReader(ConfigLoader.getParentPlace()));
        parentPlaceBr.readLine();
        String PLACE_PARENT_TABLE = "place_isPartOf_place";

        String [] attributeNames = {"PLACE_ID_A", "PLACE_ID_B"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(PLACE_PARENT_TABLE, attributeNames, attributeTypes, parentPlaceBr);
    }

    public static void parsePersonsInterests() throws IOException {
        BufferedReader personsInterestsBr = new BufferedReader(new FileReader(ConfigLoader.getPersonsInterestsPath()));
        personsInterestsBr.readLine();
        String PERSON_INTEREST_TABLE = "person_hasInterest_tag";

        String [] attributeNames = {"PERSON_ID", "TAG_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(PERSON_INTEREST_TABLE, attributeNames, attributeTypes, personsInterestsBr);
    }

    public static void parseTags() throws IOException {
        BufferedReader tagBr = new BufferedReader(new FileReader(ConfigLoader.getTagPath()));
        tagBr.readLine();
        String TAG_TABLE = "tag";

        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(100)", "VARCHAR(255)"};
        createAndFillTable(TAG_TABLE, attributeNames, attributeTypes, tagBr);
    }

    public static void parseTagClasses() throws IOException {
        BufferedReader tagClassBr = new BufferedReader(new FileReader(ConfigLoader.getTagClassPath()));
        tagClassBr.readLine();
        String TAG_CLASS_TABLE = "tagclass";

        String [] attributeNames = {"ID", "NAME", "URL"};
        String [] attributeTypes = {"BIGINT", "VARCHAR(100)", "VARCHAR(255)"};
        createAndFillTable(TAG_CLASS_TABLE, attributeNames, attributeTypes, tagClassBr);
    }

    public static void parseTagTypes() throws IOException {
        BufferedReader tagTypeBr = new BufferedReader(new FileReader(ConfigLoader.getTagTypePath()));
        tagTypeBr.readLine();
        String TAG_TYPE_TABLE = "tag_hasType_tagclass";

        String [] attributeNames = {"TAG_ID", "TAG_CLASS_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(TAG_TYPE_TABLE, attributeNames, attributeTypes, tagTypeBr);
    }

    public static void parseTagIsSubclasses() throws IOException {
        BufferedReader tagIsSubclassBr = new BufferedReader(new FileReader(ConfigLoader.getTagIsSubclassPath()));
        tagIsSubclassBr.readLine();
        String TAG_IS_SUBCLASS_TABLE = "tagclass_isSubclassOf_tagclass";

        String [] attributeNames = {"TAG_CLASS_ID", "PARENT_TAG_CLASS_ID"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(TAG_IS_SUBCLASS_TABLE, attributeNames, attributeTypes, tagIsSubclassBr);
    }

    public static void parsePersonKnowsPerson() throws IOException {
        BufferedReader personKnowsPersonBr = new BufferedReader(new FileReader(ConfigLoader.getPersonKnowsPersonPath()));
        personKnowsPersonBr.readLine();
        String PERSON_KNOWS_PERSON_TABLE = "person_knows_person";

        String [] attributeNames = {"PERSON_ID_A", "PERSON_ID_B"};
        String [] attributeTypes = {"BIGINT", "BIGINT"};
        createAndFillTable(PERSON_KNOWS_PERSON_TABLE, attributeNames, attributeTypes, personKnowsPersonBr);
    }
}
