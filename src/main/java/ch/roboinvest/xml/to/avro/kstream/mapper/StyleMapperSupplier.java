package ch.roboinvest.xml.to.avro.kstream.mapper;

import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import org.apache.kafka.streams.kstream.ValueMapper;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.function.Supplier;

public class StyleMapperSupplier implements Supplier<ValueMapper<Envelope<String>, Envelope<String>>> {

    private Transformer transformer;


    public StyleMapperSupplier(String pathToStyleSheet) throws TransformerConfigurationException, FileNotFoundException {
        transformer = createTransformer(pathToStyleSheet);
        transformer.setOutputProperty(OutputKeys.INDENT, "no");
    }

    @Override
    public ValueMapper<Envelope<String>, Envelope<String>> get() {
        return this::applyXslOnValue;
    }


    private Envelope<String> applyXslOnValue(Envelope<String> envelope) {
        if (envelope == null) return new Envelope<>("Xml Message is null", new NullPointerException());
        try {
            Source source = new StreamSource(new StringReader(envelope.getValue()));
            StringWriter writer = new StringWriter();
            Result result = new StreamResult(writer);

            transformer.transform(source, result);
            return envelope.withValue(writer.toString());
        } catch (Exception e) {
            return envelope.withAdditionalException(e);
        }
    }

    private static Transformer createTransformer(String xsltPath) throws TransformerConfigurationException, FileNotFoundException {
        TransformerFactory factory = TransformerFactory.newInstance();
        File schemaFile = new File(xsltPath);
        Templates template = factory.newTemplates(new StreamSource(new FileReader(schemaFile)));
        return template.newTransformer();
    }
}
