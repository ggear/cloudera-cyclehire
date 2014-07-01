package com.cloudera.cycelhire.main.common.model;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class PartitionRecord {

  public static final String COLUMNS_RECORD = "station";
  public static final Set<String> COLUMNS_RECORD_DETAIL = new LinkedHashSet<String>(
      Arrays.asList(new String[] { "id", "name", "terminalName", "lat", "long",
          "installed", "locked", "installDate", "removalDate", "temporary",
          "nbBikes", "nbEmptyDocks", "nbDocks" }));

  private static final SAXParserFactory XML_SAX = SAXParserFactory
      .newInstance();
  private static final XMLInputFactory XML_EVENT = XMLInputFactory
      .newInstance();

  private PartitionKey key;
  private String xml;
  private List<List<String>> table;

  public PartitionRecord key(PartitionKey key) {
    this.key = key;
    return this;
  }

  public PartitionRecord xml(String xml) {
    this.xml = xml;
    this.table = null;
    return this;
  }

  public PartitionRecord epochUpdate(String xml) {
    xml(xml);
    long epochUpdate = key == null ? 0L : key.getEpochGet();
    try {
      if (xml != null) {
        epochUpdate = Long.parseLong(XML_EVENT
            .createXMLEventReader(new StringReader(this.xml)).nextTag()
            .asStartElement().getAttributeByName(new QName("lastUpdate"))
            .getValue());
      }
    } catch (NumberFormatException | XMLStreamException
        | FactoryConfigurationError e) {
      // ignore exception, rely on default
    }
    key.epochUpdate(epochUpdate);
    return this;
  }

  public boolean isValid() {
    return key != null && xml != null && xml.length() > 0
        && !getTable().isEmpty();
  }

  public List<List<String>> getTable() {
    if (this.table == null) {
      final List<List<String>> table = new ArrayList<List<String>>();
      try {
        if (xml != null) {
          XML_SAX.newSAXParser().parse(new InputSource(new StringReader(xml)),
              new DefaultHandler() {

                private String name;
                private Map<String, String> tokens;

                @Override
                public void startElement(String uri, String localName,
                    String qName, Attributes attributes) throws SAXException {
                  if (COLUMNS_RECORD.equals(qName)) {
                    tokens = new HashMap<String, String>();
                  } else if (COLUMNS_RECORD_DETAIL.contains(qName)) {
                    name = qName;
                  }
                }

                @Override
                public void characters(char[] ch, int start, int length)
                    throws SAXException {
                  if (name != null) {
                    tokens.put(name, new String(ch, start, length));
                  }
                }

                @Override
                public void endElement(String uri, String localName,
                    String qName) throws SAXException {
                  if (COLUMNS_RECORD.equals(qName)) {
                    List<String> row = new ArrayList<String>();
                    table.add(row);
                    for (String column : COLUMNS_RECORD_DETAIL) {
                      row.add(tokens.containsKey(column) ? tokens.get(column)
                          : "");
                    }
                  } else if (COLUMNS_RECORD_DETAIL.contains(qName)) {
                    name = null;
                  }
                }

              });
        }
      } catch (ParserConfigurationException | SAXException | IOException e) {
        table.clear();
      } finally {
        this.table = table;
      }
    }
    return table;
  }

  public PartitionKey getKey() {
    return key;
  }

  public String getXml() {
    return xml;
  }

}
