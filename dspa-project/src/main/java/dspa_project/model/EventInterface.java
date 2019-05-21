package dspa_project.model;

import java.text.*;
import java.util.Date;
import java.util.Locale;

public abstract class EventInterface {
    private long id;
    private long personId;
    private Date creationDate;
    private static final String DATA_FORMAT1 = "yyyy-MM-dd'T'HH:mm:ss.'000Z'";
    private static final String DATA_FORMAT2 = "yyyy-MM-dd'T'HH:mm:ss'Z'";
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

    private String printDate(Date d){
        int milis = (int) (d.getTime() % 1000l);
        milis = milis<0 ? milis+1000 : milis;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DecimalFormat formater = new DecimalFormat("000");
        String milis_formated = formater.format(milis);
        return dateFormat.format(d) + ":" + milis_formated;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ", Data:"+ printDate(this.getCreationDate()) + ", postID:" + this.getId()+", personID:" + this.getPersonId();
    }

    protected long parseId(String data){
        return data.equals("") ? -1 : Long.parseLong(data);
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

    public abstract long getPostId();
}
