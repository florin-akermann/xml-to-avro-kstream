import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class XmlToAvroTest {


    @Test
    void test(){

        Properties properties = new Properties();
        properties.put("input-topic", "input");
        properties.put("output-topic", "output");
        properties.put("application.id", "xmlToAvroApp");
        properties.put("bootstrap.servers", "mock");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put("schema.registry.url", "http://my-schema-registry:8081");


        Topology topology = new XmlToAvroStream().topology(properties);
        TopologyTestDriver td = new TopologyTestDriver(topology, properties);
        TestInputTopic<String, String> inputTopic = td.createInputTopic("input", Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, String> outputTopic = td.createOutputTopic("output", Serdes.String().deserializer(), Serdes.String().deserializer());
        Assertions.assertTrue(outputTopic.isEmpty());
        inputTopic.pipeInput("key1", "<fo><bar>test</bar></fo>");
        Assertions.assertEquals(outputTopic.readValue(),"{\"bar\":\"test\"}");

    }
}
