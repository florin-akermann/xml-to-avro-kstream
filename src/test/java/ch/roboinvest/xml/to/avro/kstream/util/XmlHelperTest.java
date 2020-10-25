package ch.roboinvest.xml.to.avro.kstream.util;

import ch.roboinvest.xml.to.avro.kstream.util.XmlHelper;
import ch.roboinvest.xml.to.avro.kstream.util.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

class XmlHelperTest {

    @Test
    void fromFlatXml() throws ParserConfigurationException, SAXException, IOException {

        List<Pair<String, String>> expected = Arrays.asList(
                new Pair<>("bar", "foo"),
                new Pair<>("rab", "oof")
        );

        List<Pair<String, String>> actual = XmlHelper.fromFlatXml(
                "<main><bar>foo</bar><rab>oof</rab></main>"
        );

        Assertions.assertEquals(expected, actual);
    }
}