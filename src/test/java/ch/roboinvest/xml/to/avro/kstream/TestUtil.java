package ch.roboinvest.xml.to.avro.kstream;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class TestUtil {

    private TestUtil() {
    }

    public static String getAbsolutePath(String name) throws URISyntaxException {
        URL res = TestUtil.class.getClassLoader().getResource(name);
        File file = Paths.get(res.toURI()).toFile();
        return file.getAbsolutePath();
    }
}
