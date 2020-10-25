package ch.roboinvest.xml.to.avro.kstream.mapper;

import ch.roboinvest.xml.to.avro.kstream.TestUtil;
import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.net.URISyntaxException;

class ValidationMapperSupplierTest {

    @Test
    void happyPath() throws URISyntaxException {
        ValueMapper<Envelope<String>, Envelope<String>> objectUnderTest = getObjectUnderTest();

        Envelope<String> expected = new Envelope<>("<foo>1</foo>", null).withIsValid(true);
        Envelope<String> actual = objectUnderTest.apply(new Envelope<>("<foo>1</foo>", null));

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void invalidCase() throws URISyntaxException {
        ValueMapper<Envelope<String>, Envelope<String>> objectUnderTest = getObjectUnderTest();

        Envelope<String> actual = objectUnderTest.apply(new Envelope<>("<foo>3</foo>", null));

        Assertions.assertFalse(actual.isValid());
        Assertions.assertTrue(actual.getException() instanceof SAXParseException);
    }

    @Test
    void invalidInput() throws URISyntaxException {
        ValueMapper<Envelope<String>, Envelope<String>> objectUnderTest = getObjectUnderTest();

        Envelope<String> actual0 = objectUnderTest.apply(null);
        Envelope<String> actual1 = objectUnderTest.apply(new Envelope<>(null, null));
        Envelope<String> actual2 = objectUnderTest.apply(new Envelope<>("!¤#<fasdfoo>3asdfasd</foo>", null));


        Assertions.assertFalse(actual0.isValid());
        Assertions.assertFalse(actual1.isValid());
        Assertions.assertFalse(actual2.isValid());
    }

    private ValueMapper<Envelope<String>, Envelope<String>> getObjectUnderTest() throws URISyntaxException {
        ValidationMapperSupplier supplier = new ValidationMapperSupplier(new File(TestUtil.getAbsolutePath("input.xsd")).getAbsolutePath());
        return supplier.get();
    }
}