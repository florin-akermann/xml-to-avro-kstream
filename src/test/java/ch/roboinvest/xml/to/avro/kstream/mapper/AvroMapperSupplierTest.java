package ch.roboinvest.xml.to.avro.kstream.mapper;

import ch.roboinvest.xml.to.avro.kstream.TestUtil;
import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.transform.TransformerConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;

class AvroMapperSupplierTest {

    private ValueMapper<Envelope<String>, Envelope<GenericRecord>> objectUnderTest;

    @BeforeEach
    void beforEach() throws IOException, URISyntaxException {
        objectUnderTest = getObjectUnderTest();
    }


    @Test
    void happyPath() throws IOException, URISyntaxException {
        GenericData.Record expectedRecord = new GenericData.Record(AvroMapperSupplier.getSchema(TestUtil.getAbsolutePath("test.avsc")));
        expectedRecord.put("field", "someValue");

        Envelope<GenericRecord> expected = new Envelope<>(expectedRecord);
        Envelope<GenericRecord> actual = objectUnderTest.apply(new Envelope<>("<main><field>someValue</field></main>"));

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void badInput() throws IOException, URISyntaxException {
        GenericData.Record expectedRecord = new GenericData.Record(AvroMapperSupplier.getSchema(TestUtil.getAbsolutePath("test.avsc")));
        expectedRecord.put("field", "someOtherVAlue");

        Envelope<GenericRecord> actual0 = objectUnderTest.apply(null);
        Envelope<GenericRecord> actual1 = objectUnderTest.apply(new Envelope<String>(null));
        Envelope<GenericRecord> actual2 = objectUnderTest.apply(new Envelope<>("adf31113"));

        Assertions.assertFalse(actual0.isValid());
        Assertions.assertFalse(actual1.isValid());
        Assertions.assertFalse(actual2.isValid());

    }

    private ValueMapper<Envelope<String>, Envelope<GenericRecord>> getObjectUnderTest() throws URISyntaxException, IOException {
        AvroMapperSupplier supplier = new AvroMapperSupplier(TestUtil.getAbsolutePath("test.avsc"));
        return supplier.get();
    }

}