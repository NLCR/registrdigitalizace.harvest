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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import org.openarchives.oai2.ListRecordsType;
import org.openarchives.oai2.ListIdentifiersType;
import org.openarchives.oai2.OAIPMHerrorType;
import org.openarchives.oai2.OAIPMHtype;
import org.openarchives.oai2.RecordType;
import org.openarchives.oai2.RequestType;
import org.openarchives.oai2.ResumptionTokenType;

/**
 *
 * @author Jan Pokorsky
 */
public class OaiParser {

    private static final Logger LOG = Logger.getLogger(OaiParser.class.getName());

    private static final QName RESPONSE_DATE = new QName("http://www.openarchives.org/OAI/2.0/", "responseDate");
    private static final QName REQUEST = new QName("http://www.openarchives.org/OAI/2.0/", "request");
    static final QName RECORD = new QName("http://www.openarchives.org/OAI/2.0/", "record");
    private static final QName LIST_RECORDS = new QName("http://www.openarchives.org/OAI/2.0/", "ListRecords");
    private static final QName LIST_IDENTIFIERS = new QName("http://www.openarchives.org/OAI/2.0/", "ListIdentifiers");
    private static final QName ERROR = new QName("http://www.openarchives.org/OAI/2.0/", "error");
    private static final QName RESUMPTION_TOKEN = new QName("http://www.openarchives.org/OAI/2.0/", "resumptionToken");
    static final QName HEADER = new QName("http://www.openarchives.org/OAI/2.0/", "header");
    static final QName METADATA = new QName("http://www.openarchives.org/OAI/2.0/", "metadata");
    static final QName ABOUT = new QName("http://www.openarchives.org/OAI/2.0/", "about");

    private long inputSize;
    private final XmlContext xmlCtx;
    private ParserIterator<?> iterator;

    private OAIPMHtype oaiType;
    private final boolean listContentStreaming;

    public OaiParser() {
        this(false);
    }

    public OaiParser(boolean listContentStreaming) {
        this(new XmlContext(), listContentStreaming);
    }

    public OaiParser(XmlContext xmlCtx, boolean listContentStreaming) {
        this.xmlCtx = xmlCtx;
        this.listContentStreaming = listContentStreaming;
    }

    public long getInputSize() {
        return inputSize;
    }

    public <T> Iterator<T> iterator(Class<T> type) {
        return iterator != null && type == iterator.getType() ?
            (Iterator<T>) iterator: null;
    }

    public OAIPMHtype getOaiType() {
        return oaiType;
    }

    private class ParsingContext {
        XMLStreamReader reader;
        XMLStreamReader lastBoundaryReader;
        InputStream harvesterInputStream;

        void close() {
            lastBoundaryReader = null;
            try {
                if (harvesterInputStream != null) {
                    if (harvesterInputStream instanceof HarvesterInputStream) {
                        HarvesterInputStream his = (HarvesterInputStream) harvesterInputStream;
                        OaiParser.this.inputSize += his.getSize();
                        LOG.log(Level.FINER, "Parsed data: {0} bytes.", his.getSize());
                    }
                    harvesterInputStream.close();
                }
            } catch (IOException ex) {
            }
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (XMLStreamException ex) {
            }
            reader = null;
            harvesterInputStream = null;
        }

        boolean isClosed() {
            return reader == null;
        }

        /**
         * Moves the boundary reader to the last event in case its events was not consumed.
         */
        void closeBoundaryReader() throws XMLStreamException {
            if (lastBoundaryReader != null) {
                while (lastBoundaryReader.hasNext()) {
                    lastBoundaryReader.next();
                }
                lastBoundaryReader = null;
            }
        }
    }

    private void reset() {
        if (this.iterator != null) {
            this.iterator.close();
            this.iterator = null;
        }
        this.oaiType = new OAIPMHtype();
    }

    private void parseOaiPmhType(InputStream stream) throws JAXBException, IOException, XMLStreamException {
        reset();
        Unmarshaller unmarshaller = xmlCtx.getUnmarshaller();

        // dočasná změna Marek
        LOG.log(Level.INFO, "  zacatek streamu: " + stream.toString());
        // konec dočasné změny
        
        // do not use XMLEventReader as it is memory intensive
        XMLStreamReader reader = xmlCtx.createStreamParser(stream, null);
        // dočasná změna Marek
        LOG.log(Level.INFO, "  input stream: " + reader.toString());
        // konec dočasné změny
        boolean iteratorEnabled = false;
        QName name;
        try {
            findStartElement(reader, RESPONSE_DATE);
            // <responseDate>
            JAXBElement<XMLGregorianCalendar> responseDateElm =
                    unmarshaller.unmarshal(reader, XMLGregorianCalendar.class);
            this.oaiType.setResponseDate(responseDateElm.getValue());
            // <request>
            moveToStartElement(reader, REQUEST);
            name = reader.getName();
            JAXBElement<RequestType> requestElm = unmarshaller.unmarshal(reader, RequestType.class);
            this.oaiType.setRequest(requestElm.getValue());
            // choice <ListRecords>, <ListIdentifiers>, ...
            if (!moveToStartElement(reader)) {
                throw new XMLStreamException("Missing verb element.");
            }
            name = reader.getName();
            if (LIST_RECORDS.equals(name)) { // XXX here could go LIST_IDENTIFIERS
                // stop and create iterator
                ParsingContext parsingCtx = new ParsingContext();
                parsingCtx.harvesterInputStream = stream;
                parsingCtx.reader = reader;
                this.oaiType.setListRecords(new ListRecordsType());
                this.iterator = new ParserIterator(this, parsingCtx, Record.class);
                iteratorEnabled = true;
                return;

            } else if (ERROR.equals(name)) {
                List<OAIPMHerrorType> errors = oaiType.getError();
                do {
                    JAXBElement<OAIPMHerrorType> errorElm =
                            unmarshaller.unmarshal(reader, OAIPMHerrorType.class);
                    errors.add(errorElm.getValue());
                    if (!moveToStartElement(reader)) {
                        break;
                    }
                    name = reader.getName();
                } while (ERROR.equals(name));
            } else if (LIST_IDENTIFIERS.equals(name)) {
                ParsingContext parsingCtx = new ParsingContext();
                parsingCtx.harvesterInputStream = stream;
                parsingCtx.reader = reader;
                this.oaiType.setListIdentifiers(new ListIdentifiersType());
                this.iterator = new ParserIterator(this, parsingCtx, Record.class);
                iteratorEnabled = true;
                return;
            } else {
                throw new XMLStreamException("unsupported verb: " + name, reader.getLocation());
            }

            memory();
        } finally {
            if (!iteratorEnabled) {
                try {
                    reader.close();
                } catch (XMLStreamException ex) {
                    // ignore
                }
            }
        }
    }

    /**
     * Parses content of a list verb element like ListRecords, ...
     * It is expected the parser is positioned on any tag inside
     * list verb element.
     *
     * @param <T> list item type
     * @param parsingCtx context
     * @param itemType expected list item type class
     * @return list item or {@code null} in case of end of the list
     * @throws XMLStreamException
     * @throws JAXBException
     * @throws IOException
     */
    private <T> T parseListItem(ParsingContext parsingCtx, Class<T> itemType) throws XMLStreamException, JAXBException, IOException {
        if (parsingCtx == null) {
            throw new IllegalStateException();
        }
        parsingCtx.closeBoundaryReader();

        Unmarshaller unmarshaller = xmlCtx.getUnmarshaller();
        XMLStreamReader reader = parsingCtx.reader;
        // position parser from <List*> or </record> tags
        reader.next();
        if (!moveToStartElement(reader)) {
            return null;
        }
        QName name = reader.getName();
        if (RESUMPTION_TOKEN.equals(name)) {
            parseResuptionToken(reader, unmarshaller);
            return null;
        }
        if (RECORD.equals(name)) { // ListRecords
            if (Record.class != itemType) {
                throw new XMLStreamException("Invalid type: " + itemType, reader.getLocation());
            }
            Record record = parseRecord(parsingCtx, reader, unmarshaller);
            return itemType.cast(record);
        }

        // end of list
        return null;
    }

    private void parseResuptionToken(XMLStreamReader reader, Unmarshaller unmarshaller) throws JAXBException {
        JAXBElement<ResumptionTokenType> tokenElm = unmarshaller.unmarshal(
                reader, ResumptionTokenType.class);
        ResumptionTokenType token = tokenElm.getValue();
        String tokenVal = token.getValue();
        LOG.fine(" token:" + tokenVal);
        tokenVal = (tokenVal == null || tokenVal.length() == 0) ? null : tokenVal;
        ListRecordsType listRecordsType = this.oaiType.getListRecords();
        listRecordsType.setResumptionToken(tokenVal == null ? null : token);
        memory();
    }

    private Record parseRecord(ParsingContext parsingCtx, XMLStreamReader reader, Unmarshaller unmarshaller) throws XMLStreamException, IOException, JAXBException {
        XMLStreamReader freader = null;
     
        if (this.listContentStreaming) {
            freader = new BoundaryStreamReader(reader, new BoundaryStreamFilter(RECORD));
            parsingCtx.lastBoundaryReader = freader;
            Record record = new Record(null, null, new RecordTypeParser(freader, xmlCtx));
            return record;
        } else {
            StreamDumpFilter filter = new StreamDumpFilter(true, xmlCtx, METADATA);
            freader = xmlCtx.getXMLInputFactory().createFilteredReader(reader, filter);
            JAXBElement<RecordType> recordElm = unmarshaller.unmarshal(freader, RecordType.class);
            RecordType recVal = recordElm.getValue();
            Record record = new Record(recVal, filter.getDumpedText());
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(record.toString());
            }
            return record;
        }
    }

    /**
     * Same as {@link #moveToStartElement(javax.xml.stream.XMLStreamReader, javax.xml.namespace.QName) moveToStartElement}
     * but before checking it moves the reader to next event.
     * 
     * @param reader XML reader
     * @param elmName name to check after move
     * @throws XMLStreamException parser error or no start element was found
     *          or the found element does not match {@code elmName}
     */
    public static void moveToNextStartElement(XMLStreamReader reader, QName elmName) throws XMLStreamException {
        reader.next();
        moveToStartElement(reader, elmName);
    }

    /**
     * Same as {@link #moveToStartElement(javax.xml.stream.XMLStreamReader) moveToStartElement}
     * but before checking it moves the reader to next event.
     * 
     * @param reader XML reader
     * @throws XMLStreamException parser error or no tag was found
     */
    public static void moveToNextTag(XMLStreamReader reader) throws XMLStreamException {
        reader.next();
        moveToStartElement(reader);
    }

    /**
     * Moves to the next start element. In case the present state is START_ELEMENT
     * then no move is performed.
     *
     * @param reader XML reader
     * @param elmName name to check after move
     * @throws XMLStreamException parser error or no start element was found
     *          or the found element does not match {@code elmName}
     */
    public static void moveToStartElement(XMLStreamReader reader, QName elmName) throws XMLStreamException {
        if (!moveToStartElement(reader)) {
            throw new XMLStreamException("Missing element: " + elmName);
        }

        QName name = reader.getName();
        if (!elmName.equals(name)) {
            throw new XMLStreamException("Expected: " + elmName + ", found: " + name,
                    reader.getLocation());
        }

    }

    private static void findStartElement(XMLStreamReader reader, QName elmName) throws XMLStreamException {

        while (moveToStartElement(reader)) {
            QName name = reader.getName();
            if (elmName.equals(name)) {
                return;
            }
            if (!reader.hasNext()) {
                break;
            }
            reader.next();
        }

        throw new XMLStreamException("Missing element: " + elmName);
    }

    /**
     * Moves to the next start element. In case the present state is START_ELEMENT
     * then no move is performed.
     *
     * @param reader XML reader
     * @return {@code true} if parser state is START_ELEMENT after move.
     *          {@code false} means end of stream or END_ELEMENT.
     * @throws XMLStreamException parser error
     */
    public static boolean moveToStartElement(XMLStreamReader reader) throws XMLStreamException {
        while (true) {
            int eventType = reader.getEventType();
            if (eventType == XMLEvent.END_ELEMENT) {
                return false;
            } else if(eventType == XMLEvent.START_ELEMENT) {
                return true;
            }
            if (!reader.hasNext()) {
                return false;
            }
            eventType = reader.next();
        }
    }

    public void parse(InputStream stream) throws JAXBException, IOException, XMLStreamException {
        stream = new BufferedInputStream(stream, 50 * 1024);
        HarvesterInputStream harvesterInputStream = new HarvesterInputStream(stream);
        try {
            parseOaiPmhType(harvesterInputStream);
        } finally {
            try {
                // do not close when client will iterate the result
                if (this.iterator == null) {
                    this.inputSize += harvesterInputStream.getSize();
                    LOG.log(Level.FINER, "Parsed data: {0} bytes.", harvesterInputStream.getSize());
                    stream.close();
                }
            } catch (IOException ex) {
                // ignore
            }
        }

    }

    public void close() {
        LOG.log(Level.INFO, "Total parsed data: {0} bytes.", this.inputSize);
        LOG.log(Level.FINER, "Total memory: {0} bytes.", maxMemory);
        this.inputSize = 0;
        maxMemory = 0;
        ParserIterator pi = this.iterator;
        this.iterator = null;
        this.oaiType = null;
        if (pi != null) {
            pi.close();
        }
    }

    private static final class ParserIterator<T> implements Iterator<T> {

        private final OaiParser parser;
        private final ParsingContext parsingCtx;
        private T item;
        private State state = State.INITIAL;
        private final Class<T> type;
        private enum State { INITIAL, FETCHED, CLOSED }

        public ParserIterator(OaiParser parser, ParsingContext parsingCtx, Class<T> type) {
            this.parser = parser;
            this.parsingCtx = parsingCtx;
            this.type = type;
        }

        private void fetchItem() {
            if (state == State.INITIAL) {
                try {
                    item = parser.parseListItem(parsingCtx, type);
                    if (item != null) {
                        state = State.FETCHED;
                    } else {
                        state = State.CLOSED;
                        close();
                    }
                    state = item != null ? State.FETCHED : State.CLOSED;
                } catch (XMLStreamException ex) {
                    throw new RuntimeException(ex);
                } catch (JAXBException ex) {
                    throw new RuntimeException(ex);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public boolean hasNext() {
            fetchItem();
            return item != null;
        }

        @Override
        public T next() {
            if (hasNext()) {
                state = State.INITIAL;
                return item;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

        public void close() {
            parsingCtx.close();
        }

        public Class<T> getType() {
            return type;
        }

    }

    static void memory() {
        memory(false);
    }

    static long maxMemory = 0;

    static void memory(boolean gc) {
        if (gc) {
            for (int i = 0; i < 5; i++) {
                System.gc();
            }
        }
        Runtime r = Runtime.getRuntime();
        long mem = r.totalMemory() - r.freeMemory();
        maxMemory = Math.max(mem, maxMemory);
        LOG.log(Level.FINER, "Free Memory: {0} B.", mem);
    }

}
