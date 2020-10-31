package ch.roboinvest.xml.to.avro.kstream;

import ch.roboinvest.xml.to.avro.kstream.topology.XmlToAvroTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

import javax.xml.transform.TransformerConfigurationException;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class XmlToAvroStream {

    public static KafkaStreams init() throws IOException, TransformerConfigurationException {
        Properties properties = new Properties();
        return new KafkaStreams(new XmlToAvroTopology().create(properties), properties);
    }

    public static void main(String[] args) throws IOException, TransformerConfigurationException {
        KafkaStreams kstream = init();
        log.info("kstream initialized");

        log.info("starting kstream");
        kstream.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down kstream");
            kstream.close();
            log.info("kstream shut down complete");
        }));
    }
}

