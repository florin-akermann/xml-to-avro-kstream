package ch.roboinvest.xml.to.avro.kstream.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class PropHelper {

    private static final List<String> requiredProps = Arrays.asList(
            "input-topic",
            "output-topic",
            "dead-letter-queue",
            "xsl-file",
            "avro-file"
    );

    private PropHelper() {
    }

    public static Properties loadProps() {
        try (InputStream input = PropHelper.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties props = new Properties();
            if (input == null) {
                log.warn("config.properties not found");
            } else {
                props.load(input);
            }
            props.putAll(System.getProperties());
            validateProps(props);
            return props;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void validateProps(Properties props) {
        for(String prop: requiredProps){
            if (!props.containsKey(prop)) throw new IllegalStateException("No property provided fro: " + prop);
        }
    }
}
