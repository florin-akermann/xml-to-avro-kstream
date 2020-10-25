import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class XmlToAvroStream {

    public Topology topology(Properties properties) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(properties.getProperty("input-topic"));
        stream.map(KeyValue::pair)
                .to(properties.getProperty("output-topic"));
        return builder.build();
    }
}
