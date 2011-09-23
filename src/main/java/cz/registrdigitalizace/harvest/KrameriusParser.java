/*
 * Copyright (C) 2011 Jan Pokorsky
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package cz.registrdigitalizace.harvest;

import cz.registrdigitalizace.harvest.db.Metadata;
import cz.registrdigitalizace.harvest.metadata.DigobjectType;
import cz.registrdigitalizace.harvest.metadata.ModsMetadataParser;
import cz.registrdigitalizace.harvest.oai.BoundaryStreamFilter;
import cz.registrdigitalizace.harvest.oai.BoundaryStreamReader;
import cz.registrdigitalizace.harvest.oai.MetadataParser;
import cz.registrdigitalizace.harvest.oai.OaiParser;
import cz.registrdigitalizace.harvest.oai.StreamDumpFilter;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stax.StAXSource;

/**
 * Parses Kramerius 4 OAI export format.
 *
 * @author Jan Pokorsky
 */
public final class KrameriusParser implements MetadataParser<HarvestedRecord> {

    private static final Logger LOG = Logger.getLogger(KrameriusParser.class.getName());

    private static final String DR_NS_URI = "http://registrdigitalizace.cz/schemas/drkramerius/v4";
    private static final String MODS_NS_URI = "http://www.loc.gov/mods/v3";
    public static final QName RECORD_QNAME = new QName(DR_NS_URI, "record");
    public static final String RECORD_ROOT_ATTR_NAME = "root";
    public static final QName UUID_QNAME = new QName(DR_NS_URI, "uuid");
    public static final QName TYPE_QNAME = new QName(DR_NS_URI, "type");
    public static final QName DESCRIPTOR_QNAME = new QName(DR_NS_URI, "descriptor");
    public static final QName RELATION_QNAME = new QName(DR_NS_URI, "relation");
    
    public static final QName MODS_QNAME = new QName(MODS_NS_URI, "mods");
    public static final QName MODS_COLLECTION_QNAME = new QName(MODS_NS_URI, "modsCollection");

    private final XmlContext xmlContext;
    private XMLStreamReader reader;
    private final ModsMetadataParser descriptorParser;

    public KrameriusParser(XmlContext xmlContext, ModsMetadataParser descriptorParser) {
        this.xmlContext = xmlContext;
        this.descriptorParser = descriptorParser;
    }

    @Override
    public HarvestedRecord parse(XMLStreamReader reader) throws XMLStreamException {
        if (reader == null) {
            throw new NullPointerException("reader");
        }
        this.reader = reader;
        // find first <record>
        OaiParser.moveToNextStartElement(reader, RECORD_QNAME);
        HarvestedRecord record = parseRecord();
        return record;
    }

    private HarvestedRecord parseRecord() throws XMLStreamException {
        HarvestedRecord record = new HarvestedRecord();

        // ensure we are on <record>
        OaiParser.moveToStartElement(reader, RECORD_QNAME);
        // root
        String rootAttr = reader.getAttributeValue(null, RECORD_ROOT_ATTR_NAME);
        boolean root = Boolean.parseBoolean(rootAttr);
        // uuid
        OaiParser.moveToNextStartElement(reader, UUID_QNAME);
        String uuid = reader.getElementText().trim();
        record.setUuid(uuid);
        // type
        OaiParser.moveToNextStartElement(reader, TYPE_QNAME);
        String type = reader.getElementText().trim();
        // mods
        parseDescriptor(record);
        // relations
        reader.next();
        List<String> relations = parseRelations();

        record.setType(type);
        record.setRoot(root);
        record.setChildren(relations);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(record.toString());
        }

        return record;
    }

    private List<String> parseRelations() throws XMLStreamException {
        List<String> relations = new ArrayList<String>();
        while (OaiParser.moveToStartElement(reader)) {
            if (RELATION_QNAME.equals(reader.getName())) {
                String uuid = reader.getElementText().trim();
                relations.add(uuid);
                reader.next();
            }
        }
        return relations;
    }

    private void parseDescriptor(HarvestedRecord record) throws XMLStreamException {
        OaiParser.moveToNextStartElement(reader, DESCRIPTOR_QNAME);
        reader.next();
        if (!OaiParser.moveToStartElement(reader)) {
            throw new XMLStreamException("Missing descriptor content", reader.getLocation());
        }
        XMLInputFactory inFactory = xmlContext.getXMLInputFactory();
        XMLStreamReader boundaryReader =
                new BoundaryStreamReader(reader, new BoundaryStreamFilter(reader.getName(), true));
        try {
            StreamDumpFilter dumpFilter = new StreamDumpFilter(true, xmlContext, reader.getName(), true, false);
            XMLStreamReader filteredReader = inFactory.createFilteredReader(
                    boundaryReader, dumpFilter);
            // read content
            DigobjectType dt = descriptorParser.parse(new StAXSource(filteredReader));
            Metadata metadata = Metadata.create(dt, null);
            record.setMetadata(metadata);
            String descriptor = dumpFilter.getDumpedText();
            record.setDescriptor(descriptor);
            if (dt == null) {
                LOG.log(Level.WARNING, "unexpected metadata content for {0}:\n{1}", new Object[] {record.getUuid(), descriptor});
            }
        } catch (IOException ex) {
            throw new XMLStreamException("missing allocator", reader.getLocation(), ex);
        }

        if (reader.getEventType() != XMLStreamReader.END_ELEMENT) {
            reader.nextTag(); // skip garbage and find </descriptor>
        }

        if (reader.getEventType() != XMLStreamReader.END_ELEMENT || !DESCRIPTOR_QNAME.equals(reader.getName())) {
            throw new XMLStreamException("Expected </descriptor>", reader.getLocation());
        }
    }

}
