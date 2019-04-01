package dspa_project.schemas;

import dspa_project.model.EventInterface;
import java.util.Map;

// A MISERABLE ATTEMPT TO CREATE A GENERIC SCHEMA :D

public class SerializationSchema<T extends EventInterface> {
    private boolean isKey;

    public void configure(Map<String, ?> configs, boolean isKey)
    {
        this.isKey = isKey;
    }

    public byte[] serialize(String topic, T event)
    {
        return event.serialize();
    }

    public T deserialize(String s, byte[] bytes)
    {
        return (T) T.deserialize(bytes);
    }

    public void close()
    {

    }
}
