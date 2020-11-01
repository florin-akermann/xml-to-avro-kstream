package ch.roboinvest.xml.to.avro.kstream.topology;

import ch.roboinvest.xml.to.avro.kstream.mapper.AvroMapperSupplier;
import ch.roboinvest.xml.to.avro.kstream.mapper.StyleMapperSupplier;
import ch.roboinvest.xml.to.avro.kstream.mapper.ValidationMapperSupplier;
import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import ch.roboinvest.xml.to.avro.kstream.util.PropHelper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;

@Slf4j
public class XmlToAvroTopology {

    private org.apache.avro.Schema validationErrorSchema;
    private org.apache.avro.Schema deadLetterQueueSchema;
    private Properties props;
    private Serde<GenericRecord> serde;

    public Topology create(Properties properties) throws IOException, TransformerConfigurationException {
        return create(properties, createGenericAvroSerde(properties));
    }

    private Serde<GenericRecord> createGenericAvroSerde(Properties properties) {
        GenericAvroSerde serde = new GenericAvroSerde();
        serde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG)),
                false
        );
        return serde;
    }

    public Topology create(Properties properties, Serde<GenericRecord> genericRecordSerde) throws IOException, TransformerConfigurationException {
        PropHelper.validateProps(properties);

        this.props = properties;
        this.serde = genericRecordSerde;
        this.validationErrorSchema = new Parser().parse(getClass().getClassLoader().getResourceAsStream("validation-error.avsc"));
        this.deadLetterQueueSchema = new Parser().parse(getClass().getClassLoader().getResourceAsStream("dead-letter-avro.avsc"));

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Envelope<String>> source = getSourceMessagesInEnvelopes(builder);

        KStream<String, Envelope<String>> postValidation = viaValidationFlow(source);
        invalidXmlAlsoToInvalidErrorQueue(postValidation);

        KStream<String, Envelope<String>> postValidationAndTransform = viaTransformFlow(postValidation);
        failedTransformAlsoToDeadLetterQueue(postValidationAndTransform);

        KStream<String, Envelope<GenericRecord>> avroSource = viaMapToAvroFlow(postValidationAndTransform);
        publishSuccessfulEnvelopes(avroSource);
        publishErroneousMessageToDlq(avroSource);

        return builder.build();
    }


    private KStream<String, Envelope<String>> getSourceMessagesInEnvelopes(StreamsBuilder builder) {
        return builder.<String, String>stream(getTopics(props.getProperty("input-topic")))
                .mapValues(v -> new Envelope<>(v, null));
    }

    private void publishErroneousMessageToDlq(KStream<String, Envelope<GenericRecord>> avroSource) {
        avroSource.filter((k, v) -> !v.success())
                .map(this::getDeadLetterQueueRecord)
                .peek((k, v) -> log.info("publishing to dead-letter-queue topic key: {} - value:{}", k, v))
                .to(props.getProperty("dead-letter-queue"), Produced.valueSerde(serde));
    }

    private void publishSuccessfulEnvelopes(KStream<String, Envelope<GenericRecord>> avroSource) {
        avroSource
                .filter((k, v) -> v.success())
                .mapValues(Envelope::getValue)
                .peek((k, v) -> log.info("publishing to out-put topic key: {} - value:{}", k, v))
                .to(props.getProperty("output-topic"), Produced.valueSerde(serde));
    }

    private KStream<String, Envelope<GenericRecord>> viaMapToAvroFlow(KStream<String, Envelope<String>> postValidationAndTransform) throws IOException {
        return postValidationAndTransform.filter((k, v) -> v.success())
                .mapValues(new AvroMapperSupplier(props.getProperty("avro-file")).get());
    }

    private void failedTransformAlsoToDeadLetterQueue(KStream<String, Envelope<String>> postValidationAndTransform) {
        postValidationAndTransform.filter((k, v) -> !v.success())
                .map(this::getDeadLetterQueueRecord)
                .peek((k, v) -> log.info("publishing to dead-letter-queue topic key: {} - value:{}", k, v))
                .to(props.getProperty("dead-letter-queue"), Produced.valueSerde(serde));
    }

    private KStream<String, Envelope<String>> viaTransformFlow(KStream<String, Envelope<String>> postValidation) throws TransformerConfigurationException, FileNotFoundException {
        return postValidation
                .mapValues(new StyleMapperSupplier(props.getProperty("xsl-file")).get());
    }

    private void invalidXmlAlsoToInvalidErrorQueue(KStream<String, Envelope<String>> postValidation) {
        postValidation.filter((k, v) -> v.validationApplied() && !v.isValid())
                .map(this::getValidationErrorRecord)
                .peek((k, v) -> log.info("publishing to validation-error topic key: {} - value:{}", k, v))
                .to(props.getProperty("validation-error-topic"), Produced.valueSerde(serde));
    }

    private KStream<String, Envelope<String>> viaValidationFlow(KStream<String, Envelope<String>> source) {
        if (props.containsKey("xsd-file")) {
            return source.mapValues(new ValidationMapperSupplier(props.getProperty("xsd-file")).get());
        } else {
            log.info("no xsd-file path provided: xsd-validation disabled");
            return source;
        }
    }

    private KeyValue<String, GenericRecord> getDeadLetterQueueRecord(String k, Envelope<? extends Object> v) {
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
