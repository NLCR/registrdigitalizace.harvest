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

import java.io.CharArrayWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.namespace.QName;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.XMLEventAllocator;

/**
 * Dumps and filters XML element contents.
 *
 * @author Jan Pokorsky
 */
public final class StreamDumpFilter implements StreamFilter {

    private enum State {SEARCHING, METADATA}

    private static final Logger LOG = Logger.getLogger(StreamDumpFilter.class.getName());

    private QName trigger;
    private final boolean skipWhitespaces;
    private State state = State.SEARCHING;
    private XMLEventWriter writer;
    private static final CharArrayWriter charWriter = new CharArrayWriter(2048);
    private final XMLEventAllocator allocator;
    private final XMLOutputFactory xmlOutFactory;
    private final XMLEventFactory xmlEventFactory;

    public StreamDumpFilter(boolean skipWhitespaces, XmlContext xmlContext, QName trigger) throws IOException {
        this.allocator = xmlContext.getXMLEventAllocator();
        this.xmlOutFactory = xmlContext.getXMLOutputFactory();
        this.xmlEventFactory = xmlContext.getXMLEventFactory();
        this.skipWhitespaces = skipWhitespaces;
        this.trigger = trigger;
    }

    public String getDumpedText() {
        return charWriter.toString();
    }

    @Override
    public boolean accept(XMLStreamReader reader) {
        if (state == State.SEARCHING) {
            if (reader.getEventType() == XMLStreamReader.START_ELEMENT) {
                if (isTriggerMatched(reader.getName())) {
                    state = State.METADATA;
                    initWriter();
                }
            }
        } else if (state == State.METADATA) {
            if (reader.getEventType() == XMLStreamReader.END_ELEMENT) {
                if (isTriggerMatched(reader.getName())) {
                    state = State.SEARCHING;
                    closeWriter();
                    return true;
                }
            }
            writeEvent(reader);
            return false;
        }
        return true;
    }

    private boolean isTriggerMatched(QName name) {
        return name.equals(trigger);
    }

    private void initWriter() {
        try {
            charWriter.reset();
            writer = xmlOutFactory.createXMLEventWriter(charWriter);
            writer.add(xmlEventFactory.createStartDocument());
        } catch (XMLStreamException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    private void closeWriter() {
        if (writer != null) {
            try {
                writer.add(xmlEventFactory.createEndDocument());
                writer.flush();
                charWriter.flush();
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("content: " + charWriter.size() + ", " + charWriter.toString());
                }
                writer = null;
            } catch (XMLStreamException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }

    private void writeEvent(XMLStreamReader reader) {
        if (writer != null) {
            try {
                XMLEvent event = allocator.allocate(reader);
                if (skipWhitespaces && isWhitespaces(event)) {
                    return;
                }
//                LOG.fine(event.getEventType() + ", " + event.getClass() + ", " + event.toString());
                writer.add(event);
            } catch (XMLStreamException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }

    private boolean isWhitespaces(XMLEvent event) {
        if (event.getEventType() == XMLEvent.CHARACTERS) {
            Characters asCharacters = event.asCharacters();
            if (asCharacters.isWhiteSpace()) {
                return true;
            }
        }
        return false;
    }

}
