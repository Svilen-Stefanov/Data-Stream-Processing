package dspa_project.stream.sinks;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteOutputFormat implements OutputFormat<String> {
    private String pathToFile;
    private final String csvHeader;
    File file;

    private static final long serialVersionUID = 1L;

    public WriteOutputFormat(String path, String header){
        this.pathToFile = path;
        this.csvHeader = header;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.file = new File(this.pathToFile);
        this.file.getParentFile().mkdirs();
        this.file.createNewFile();
        writeRecord(this.csvHeader);
    }

    @Override
    public void writeRecord(String record) {
        FileWriter fr = null;
        try {
            // Below constructor argument decides whether to append or override
            fr = new FileWriter(this.file, true);
            fr.write(record + "\n");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
    }
}
