package dspa_project.schemas;

import dspa_project.event.PostEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class PostSchema implements DeserializationSchema<PostEvent>, SerializationSchema<PostEvent> {
    @Override
    public PostEvent deserialize(byte[] bytes) throws IOException {
        return PostEvent.deserialize(bytes);
    }

    @Override
    public byte[] serialize(PostEvent event) {
        return event.serialize(event);
    }

    @Override
    public TypeInformation<PostEvent> getProducedType() {
        return TypeExtractor.getForClass(PostEvent.class);
    }

    // Method to decide whether the element signals the end of the stream.
    // If true is returned the element won't be emitted.
    @Override
    public boolean isEndOfStream(PostEvent event) {
        return false;
    }
}
