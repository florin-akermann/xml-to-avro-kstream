package ch.roboinvest.xml.to.avro.kstream.util;

import ch.roboinvest.xml.to.avro.kstream.util.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XmlHelper {

    public static List<Pair<String, String>> fromFlatXml(String xmlString) throws IOException, SAXException, ParserConfigurationException {
        List<Pair<String,String>> toReturn = new ArrayList<>();
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document dom = db.parse(new ByteArrayInputStream(xmlString.getBytes()));
        Element docEle = dom.getDocumentElement();
        NodeList nl = docEle.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            toReturn.add(new Pair<>(nl.item(i).getNodeName(), nl.item(i).getTextContent()));
        }
        return toReturn;
    }
}
