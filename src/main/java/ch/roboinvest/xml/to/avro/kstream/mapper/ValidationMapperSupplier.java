package ch.roboinvest.xml.to.avro.kstream.mapper;

import ch.roboinvest.xml.to.avro.kstream.util.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.StringReader;
import java.util.function.Supplier;

@Slf4j
public class ValidationMapperSupplier implements Supplier<ValueMapper<Envelope<String>, Envelope<String>>> {

    private final Validator validator;

    public ValidationMapperSupplier(String xsdFilePath) {
        validator = createValidator(xsdFilePath);
    }

    @Override
    public ValueMapper<Envelope<String>, Envelope<String>> get() {
        return this::xsdValidationOnValue;
    }

    private Envelope<String> xsdValidationOnValue(Envelope<String> envelope) {
        if (envelope== null) return new Envelope<>("xml message was nul", new NullPointerException());
        try {
            Source xmlSource = new StreamSource(new StringReader(envelope.getValue()));
            validator.validate(xmlSource);
            return envelope
                    .withValidationApplied(true)
                    .withIsValid(true);
        } catch (Exception e) {
            log.warn("validation-step", e);
            return envelope
                    .withAdditionalException(e)
                    .withValidationApplied(true);
        }
    }

    private Validator createValidator(String pathname) {
        try {
            File schemaFile = new File(pathname);
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = schemaFactory.newSchkema(schemaFile);
            return schema.newValidator();
        } catch (SAXException e) {
            throw new RuntimeException(e);
        }
    }
}
