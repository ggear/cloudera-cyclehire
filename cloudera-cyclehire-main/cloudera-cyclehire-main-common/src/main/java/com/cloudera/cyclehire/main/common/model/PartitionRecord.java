package com.cloudera.cyclehire.main.common.model;

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

  private static final String XML_RECORDS = "stations";
  private static final String XML_RECORDS_UPDATE = "lastUpdate";
  public static final String XML_RECORD = "station";
  public static final Set<String> XML_RECORD_COLUMNS = new LinkedHashSet<String>(Arrays.asList(new String[] { "id",
      "name", "terminalName", "lat", "long", "installed", "locked", "installDate", "removalDate", "temporary",
      "nbBikes", "nbEmptyDocks", "nbDocks" }));
  public static final List<String> XML_RECORD_EMPTY = new ArrayList<>();
  static {
    for (int i = 0; i < XML_RECORD_COLUMNS.size(); i++) {
      XML_RECORD_EMPTY.add("");
    }
  }

  private static final SAXParserFactory XML_SAX = SAXParserFactory.newInstance();
  private static final XMLInputFactory XML_EVENT = XMLInputFactory.newInstance();

  private PartitionKey key;
  private String xml;
  private boolean isValid = false;
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
        epochUpdate = Long.parseLong(XML_EVENT.createXMLEventReader(new StringReader(this.xml)).nextTag()
            .asStartElement().getAttributeByName(new QName(XML_RECORDS_UPDATE)).getValue());
      }
    } catch (NumberFormatException | XMLStreamException | FactoryConfigurationError e) {
      // ignore exception, rely on default
    }
    key.epochUpdate(epochUpdate);
    return this;
  }

  public boolean isValid() {
    return key != null && xml != null && xml.length() > 0 && !getTable().isEmpty() && isValid;
  }

  public List<List<String>> getTable() {
    if (this.table == null) {
      final List<List<String>> table = new ArrayList<List<String>>();
      try {
        if (xml != null) {
          XML_SAX.newSAXParser().parse(new InputSource(new StringReader(xml)), new DefaultHandler() {

            private String name;
            private Map<String, String> tokens;

            @Override
            public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
              if (XML_RECORDS.equals(qName)) {
                if (key != null) {
                  try {
                    if (!key.epochUpdate(Long.parseLong(attributes.getValue(XML_RECORDS_UPDATE))).isValid()) {
                      throw new SAXException("Invalid key [" + key + "]");
                    }
                  } catch (NumberFormatException exception) {
                    throw new SAXException("Invalid record timestamp [" + attributes.getValue(XML_RECORDS_UPDATE) + "]");
                  }
                }
              } else if (XML_RECORD.equals(qName)) {
                tokens = new HashMap<String, String>();
              } else if (XML_RECORD_COLUMNS.contains(qName)) {
                name = qName;
              }
            }

            @Override
            public void characters(char[] ch, int start, int length) throws SAXException {
              if (name != null) {
                tokens.put(name, new String(ch, start, length));
              }
            }

            @Override
            public void endElement(String uri, String localName, String qName) throws SAXException {
              if (XML_RECORD.equals(qName)) {
                List<String> row = new ArrayList<String>();
                table.add(row);
                for (String column : XML_RECORD_COLUMNS) {
                  row.add(tokens.containsKey(column) ? tokens.get(column) : "");
                }
              } else if (XML_RECORD_COLUMNS.contains(qName)) {
                name = null;
              }
            }

          });
        }
      } catch (ParserConfigurationException | SAXException | IOException e) {
        table.clear();
      } finally {
        this.table = table;
        if (!(isValid = !table.isEmpty())) {
          table.add(XML_RECORD_EMPTY);
        }
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
