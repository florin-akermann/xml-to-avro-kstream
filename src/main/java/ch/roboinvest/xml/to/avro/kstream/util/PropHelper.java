package ch.roboinvest.xml.to.avro.kstream.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class PropHelper {

    private static List<String> requiredProps = Arrays.asList(
            "",
            ""
    );

    private PropHelper() {
    }

    public static Properties loadProps() {
        Properties props = new Properties();
        try (InputStream input = PropHelper.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                log.warn("properties not found");
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

    }
}
