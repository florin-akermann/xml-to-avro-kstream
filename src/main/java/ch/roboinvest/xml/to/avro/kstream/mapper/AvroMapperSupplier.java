package ch.roboinvest.xml.to.avro.kstream.mapper;

import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import ch.roboinvest.xml.to.avro.kstream.util.Pair;
import ch.roboinvest.xml.to.avro.kstream.util.XmlHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

public class AvroMapperSupplier implements Supplier<ValueMapper<Envelope<String>, Envelope<GenericRecord>>> {

    private final Schema outPutAvroSchema;

    public AvroMapperSupplier(String pathToAvroSchemaFile) throws IOException {
        outPutAvroSchema = getSchema(pathToAvroSchemaFile);
    }

    @Override
    public ValueMapper<Envelope<String>, Envelope<GenericRecord>> get() {
        return this::mapToAvro;
    }

    private Envelope<GenericRecord> mapToAvro(Envelope<String> v) {
        if (v == null) return new Envelope<>(null, new NullPointerException("Envelope value was null"));
        try {
            List<Pair<String, String>> xmlPairs = XmlHelper.fromFlatXml(v.getValue());
            GenericRecord avroRecord = new GenericData.Record(outPutAvroSchema);
            for (Pair<String, String> pair : xmlPairs) {
                avroRecord.put(pair.getKey(), pair.getValue());
            }
            return new Envelope<>(avroRecord, null);
        } catch (Exception e) {
            if (v.getException()!=null){
                e.addSuppressed(v.getException());
            }
            return new Envelope<>(new GenericData.Record(outPutAvroSchema), e);
        }
    }

    public static org.apache.avro.Schema getSchema(String avroFileLocation) throws IOException {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return parser.parse(loadFromFile(avroFileLocation));
    }

    private static String loadFromFile(String avroFileLocation) throws IOException {
        return Files.readString(Path.of(avroFileLocation));
    }
}
