package ch.roboinvest.xml.to.avro.kstream.topology;

import ch.roboinvest.xml.to.avro.kstream.TestUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.xml.transform.TransformerConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;

public class XmlToAvroTopologyTest {

    // A mocked schema registry for our serdes to use
    private static final String SCHEMA_REGISTRY_SCOPE = XmlToAvroTopologyTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    @Test
    void test() throws IOException, TransformerConfigurationException, URISyntaxException {

        Properties properties = new Properties();
        properties.put("input-topic", "input");
        properties.put("output-topic", "output");
        properties.put("dead-letter-queue", "dlq");
        properties.put("validation-error-topic", "validation-error-warning");
        properties.put("application.id", "xmlToAvroApp");
        properties.put("bootstrap.servers", "mock");

        properties.put("xsd-file", TestUtil.getAbsolutePath("integration.xsd"));
        properties.put("xsl-file", TestUtil.getAbsolutePath("integration.xslt"));
        properties.put("avro-file", TestUtil.getAbsolutePath("test.avsc"));


        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put("schema.registry.url", "http://my-schema-registry:8081");

        final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
        genericAvroSerde.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL), false);
        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);

        Topology topology = new XmlToAvroTopology().create(properties);
        TopologyTestDriver td = new TopologyTestDriver(topology, properties);

        TestInputTopic<String, String> inputTopic = td.createInputTopic(
                "input",
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );

        final TestOutputTopic<String, Object> outputTopic = td.createOutputTopic(
                "output",
                new StringDeserializer(),
                new KafkaAvroDeserializer(schemaRegistryClient)
        );

        final TestOutputTopic<String, Object> dlq = td.createOutputTopic(
                properties.getProperty("dead-letter-queue"),
                new StringDeserializer(),
                new KafkaAvroDeserializer(schemaRegistryClient)
        );

        final TestOutputTopic<String, Object> validationError = td.createOutputTopic(
                properties.getProperty("validation-error-topic"),
                new StringDeserializer(),
                new KafkaAvroDeserializer(schemaRegistryClient)
        );

        Assertions.assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("someId", Files.readString(Path.of(TestUtil.getAbsolutePath("integration.xml"))));
        Assertions.assertEquals("{\"foo\": \"2\"}", outputTopic.readValue().toString());
        Assertions.assertTrue(dlq.isEmpty());
        Assertions.assertTrue(validationError.isEmpty());

        inputTopic.pipeInput("someOtherId", "<!#!thisShouldEndUpIntheValidationErrorQueue+DeadLetterQueue>");
        Assertions.assertTrue(outputTopic.isEmpty());
        Assertions.assertEquals(dlqMessage(), dlq.readValue().toString());
        Assertions.assertEquals(validationMsg(), validationError.readValue().toString());

        //TODO test case: validation ok, still on dlq - e.g. faulty stylesheet
    }

    private String dlqMessage() {
        return "{\"Exception\": " +
                "\"org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 3;" +
                " The markup in the document preceding the root element must be well-formed.\", " +
                "\"OriginalMessageKey\": \"someOtherId\"}";
    };

    private String validationMsg(){
        return "{\"ValidationError\": " +
                "\"org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 3;" +
                " The markup in the document preceding the root element must be well-formed.\", " +
                "\"OriginalMessageKey\": \"someOtherId\"}";
    }

    @Test
    void whenStyleSheetMissingThenAssertThrowsIoException() {
        Assertions.assertThrows(IOException.class, ()->{
            new XmlToAvroTopology().create(new Properties(), null);
        });
    }
}
