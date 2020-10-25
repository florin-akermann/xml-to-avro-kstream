package ch.roboinvest.xml.to.avro.kstream.mapper;

import ch.roboinvest.xml.to.avro.kstream.TestUtil;
import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.transform.TransformerConfigurationException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

class StyleMapperSupplierTest {

    private ValueMapper<Envelope<String>, Envelope<String>> objectUnderTest;

    @BeforeEach
    void beforeEach() throws FileNotFoundException, TransformerConfigurationException, URISyntaxException {
        objectUnderTest = getObjectUnderTest();
    }

    @Test
    void happyPath() throws URISyntaxException, IOException {
        Envelope<String> expected = new Envelope<>(getXml(TestUtil.getAbsolutePath("output.xml")));
        Envelope<String> actual = objectUnderTest.apply(new Envelope<>(getXml(TestUtil.getAbsolutePath("input.xml"))));
        Assertions.assertEquals(expected.getValue().replace("\\s", ""), actual.getValue().replace("\\s+", ""));
    }

    @Test
    void testInvalidInput() {
        Envelope<String> res0 = objectUnderTest.apply(null);
        Envelope<String> res1 = objectUnderTest.apply(new Envelope<>(null, null));
        Envelope<String> res2 = objectUnderTest.apply(new Envelope<>("asdfæ>asd34"));

        Assertions.assertFalse(res0.isValid());
        Assertions.assertFalse(res1.isValid());
        Assertions.assertFalse(res2.isValid());

    }

    private ValueMapper<Envelope<String>, Envelope<String>> getObjectUnderTest() throws URISyntaxException, TransformerConfigurationException, FileNotFoundException {
        StyleMapperSupplier supplier = new StyleMapperSupplier(TestUtil.getAbsolutePath("style.xslt"));
        return supplier.get();
    }

    private String getXml(String first) throws IOException {
        return Files.readString(Path.of(first));
    }
}