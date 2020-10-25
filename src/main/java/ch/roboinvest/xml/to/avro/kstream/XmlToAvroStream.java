package ch.roboinvest.xml.to.avro.kstream;

import ch.roboinvest.xml.to.avro.kstream.mapper.AvroMapperSupplier;
import ch.roboinvest.xml.to.avro.kstream.mapper.StyleMapperSupplier;
import ch.roboinvest.xml.to.avro.kstream.mapper.ValidationMapperSupplier;
import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import javax.xml.transform.TransformerConfigurationException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class XmlToAvroStream {


    private org.apache.avro.Schema validationErrorSchema;
    private org.apache.avro.Schema deadLetterQueueSchema;

    public Topology topology(Properties properties, Serde<GenericRecord> genericAvroSerde) throws IOException, TransformerConfigurationException {

        validationErrorSchema = new Parser().parse(getClass().getClassLoader().getResourceAsStream("validation-error.avsc"));
        deadLetterQueueSchema = new Parser().parse(getClass().getClassLoader().getResourceAsStream("dead-letter-avro.avsc"));

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(getTopics(properties.getProperty("input-topics")));

        KStream<String, Envelope<String>> postValidation = stream
                .mapValues(v -> new Envelope<>(v, null))
                .mapValues(new ValidationMapperSupplier(properties.getProperty("xsd-file")).get());

        postValidation.filter((k, v) -> !v.validationSuccessful())
                .map(this::getValidationErrorRecord)
                .to(properties.getProperty("validation-error-topic"), Produced.valueSerde(genericAvroSerde));

        KStream<String, Envelope<String>> postValidationAndTransform = postValidation
                .mapValues(new StyleMapperSupplier(properties.getProperty("xsl-file")).get());

        postValidationAndTransform.filter((k, v) -> v.success())
                .mapValues(new AvroMapperSupplier(properties.getProperty("avro-file")).get())
                .mapValues(Envelope::getValue)
                .to(properties.getProperty("output-topic"), Produced.valueSerde(genericAvroSerde));

        postValidationAndTransform.filter((k, v) -> !v.success())
                .map(this::getDeadLetterQueueRecord)
                .to(properties.getProperty("dead-letter-queue"), Produced.valueSerde(genericAvroSerde));

        return builder.build();
    }

    private KeyValue<String, GenericRecord> getDeadLetterQueueRecord(String k, Envelope<String> v) {
        GenericData.Record record = new GenericData.Record(deadLetterQueueSchema);
        record.put("Exception", v.getException().toString());
        record.put("OriginalMessageKey", k);
        return KeyValue.pair(k, record);
    }

    private KeyValue<String, GenericRecord> getValidationErrorRecord(String k, Envelope<String> v) {
        GenericData.Record record = new GenericData.Record(validationErrorSchema);
        record.put("ValidationError", v.getException().toString());
        record.put("OriginalMessageKey", k);
        return KeyValue.pair(k, record);
    }

    private Collection<String> getTopics(String property) {
        return Arrays.stream(property.split(",")).collect(Collectors.toList());
    }

}
