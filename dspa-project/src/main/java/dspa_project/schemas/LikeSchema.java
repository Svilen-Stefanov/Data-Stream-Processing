package dspa_project.schemas;

import dspa_project.event.LikeEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

public class LikeSchema implements DeserializationSchema<LikeEvent>, SerializationSchema<LikeEvent> {
        @Override
        public LikeEvent deserialize(byte[] bytes) throws IOException {
            return LikeEvent.deserialize(bytes);
        }

        @Override
        public byte[] serialize(LikeEvent event) {
            return event.serialize(event);
        }

        @Override
        public TypeInformation<LikeEvent> getProducedType() {
            return TypeExtractor.getForClass(LikeEvent.class);
        }

        // Method to decide whether the element signals the end of the stream.
        // If true is returned the element won't be emitted.
        @Override
        public boolean isEndOfStream(LikeEvent event) {
            return false;
        }
}
