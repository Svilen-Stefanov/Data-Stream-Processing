package dspa_project.event;

import javafx.scene.input.DataFormat;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class EventInterface {
    private long id;
    private long personId;
    private Date creationDate;
    private static final String DATA_FORMAT1 = "yyyy-MM-dd'T'hh:mm:ss.'000Z'";
    private static final String DATA_FORMAT2 = "yyyy-MM-dd'T'hh:mm:ss'Z'";
    private static SimpleDateFormat dataFormat1 = new SimpleDateFormat(DATA_FORMAT1, Locale.ENGLISH);
    private static SimpleDateFormat dataFormat2 = new SimpleDateFormat(DATA_FORMAT2, Locale.ENGLISH);

    public EventInterface(String [] data){
        this.id = Long.parseLong(data[0]);
        this.personId = Long.parseLong(data[1]);

        try {

            if (isFormat(dataFormat1, data[2])) {
                this.creationDate = dataFormat1.parse(data[2]);
            } else if (isFormat(dataFormat2, data[2])) {
                this.creationDate = dataFormat2.parse(data[2]);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public EventInterface(long id, long personId, Date creationDate) {
        this.id = id;
        this.personId = personId;
        this.creationDate = creationDate;
    }

    private static boolean isFormat(DateFormat format, String candidate) {
        return format.parse(candidate, new ParsePosition(0)) != null;
    }

    protected long parseId(String data){
        return data.equals("") ? 0 : Long.parseLong(data);
    }

    public long getId() {
        return id;
    }

    public long getPersonId() {
        return personId;
    }

    public Date getCreationDate() {
        return creationDate;
    }

}
