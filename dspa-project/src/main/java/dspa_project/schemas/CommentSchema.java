package dspa_project.schemas;

import dspa_project.event.CommentEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class CommentSchema implements DeserializationSchema<CommentEvent>, SerializationSchema<CommentEvent> {
    @Override
    public CommentEvent deserialize(byte[] bytes) throws IOException {
        return CommentEvent.deserialize(bytes);
    }

    @Override
    public byte[] serialize(CommentEvent event) {
        return event.serialize(event);
    }

    @Override
    public TypeInformation<CommentEvent> getProducedType() {
        return TypeExtractor.getForClass(CommentEvent.class);
    }

    // Method to decide whether the element signals the end of the stream.
    // If true is returned the element won't be emitted.
    @Override
    public boolean isEndOfStream(CommentEvent event) {
        return false;
    }
}
