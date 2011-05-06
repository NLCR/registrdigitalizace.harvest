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

package cz.registrdigitalizace.harvest.oai;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.openarchives.oai2.AboutType;
import org.openarchives.oai2.HeaderType;

/**
 * Helper class to parse OAI {@code <record>} element properly.
 *
 * <p>Usage:
 * <br/>First invoke {@link #parseHeader() parseHeader()}, then invoke
 * {@link #parseMetadata(cz.registrdigitalizace.harvester.oai.RecordTypeParser.Parser) parseMetadata()}
 * and if necessary invoke {@link #parseAbouts() parseAbouts()} at last.</p>
 *
 * @author Jan Pokorsky
 */
public class RecordTypeParser {

    private final XMLStreamReader reader;
    private XMLStreamReader metadataReader;
    private final XmlContext xmlContext;
    private boolean headerParsed = false;
    private boolean metadataParsed = false;
    private boolean aboutParsed = false;
    private HeaderType header;
    private List<AboutType> abouts;

    RecordTypeParser(XMLStreamReader reader, XmlContext xmlContext) {
        this.reader = reader;
        this.xmlContext = xmlContext;
    }

    public HeaderType parseHeader() throws XMLStreamException, JAXBException {
        if (headerParsed) {
            return header;
        }
        OaiParser.moveToNextStartElement(reader, OaiParser.HEADER);
        Unmarshaller unmarshaller = xmlContext.getUnmarshaller();
        JAXBElement<HeaderType> headerElm = unmarshaller.unmarshal(reader, HeaderType.class);
        header = headerElm.getValue();
        headerParsed = true;
        return header;
    }

    public <T> T parseMetadata(MetadataParser<T> metadataParser) throws XMLStreamException, JAXBException {
        if (metadataParsed) {
            return null;
        }
        parseHeader();

        metadataParsed = true;
        boolean moved = OaiParser.moveToStartElement(reader);
        if (!moved || !OaiParser.METADATA.equals(reader.getName())) {
            // no metadata element
            return null;
        }
        metadataReader = new BoundaryStreamReader(
                reader, new BoundaryStreamFilter(OaiParser.METADATA));
        T result =  metadataParser.parse(metadataReader);
        return result;
    }

    public List<AboutType> parseAbouts() throws XMLStreamException, JAXBException {
        if (aboutParsed) {
            return abouts;
        }
        if (!metadataParsed) {
            throw new XMLStreamException("Parse metadata first.");
        }

        // ensure the whole metadata element was read
        while (metadataReader.hasNext()) {
            metadataReader.next();
        }
        if (reader.hasNext()) {
            // move parser from </metadata> tag
            reader.next();
        }

        // about is not mandatory element
        abouts = new ArrayList<AboutType>();
        while (OaiParser.moveToStartElement(reader)) {
            Unmarshaller unmarshaller = xmlContext.getUnmarshaller();
            if (OaiParser.ABOUT.equals(reader.getName())) {
                JAXBElement<AboutType> aboutElm = unmarshaller.unmarshal(reader, AboutType.class);
                AboutType about = aboutElm.getValue();
                if (about != null) {
                    abouts.add(about);
                }
            } else {
                break;
            }
        }

        return abouts;
    }
}
