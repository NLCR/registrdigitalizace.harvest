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

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.stream.XMLStreamException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.openarchives.oai2.HeaderType;
import static org.junit.Assert.*;
import org.openarchives.oai2.ListRecordsType;
import org.openarchives.oai2.OAIPMHerrorType;
import org.openarchives.oai2.OAIPMHtype;
import org.openarchives.oai2.ResumptionTokenType;

/**
 *
 * @author Jan Pokorsky
 */
//@Ignore
public class OaiParserTest {

    public OaiParserTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testListOneRecord() throws Exception {
        doTestListOneRecord("testListOneRecord.xml");
        doTestListOneRecordStream("testListOneRecord.xml");
    }

    @Test
    public void testListOneRecordCollapsedXml() throws Exception {
        doTestListOneRecord("testListOneRecordCollapsed.xml");
    }

    private void doTestListOneRecord(String filename) throws Exception {
        OaiParser parser = initParser(filename, HeaderType.class, false);
        Iterator<Record> iterator = parser.iterator(Record.class);
        assertNotNull("missing record iterator", iterator);
        assertTrue("no record data", iterator.hasNext());
        Record next = iterator.next();
        assertNotNull("missing record", next);
        assertFalse("unexpected hasNext", iterator.hasNext());
        try {
            Record next2 = iterator.next();
            fail("unexpected record: " + next2);
        } catch (NoSuchElementException ex) {
            // it's expected
        }
    }

    private void doTestListOneRecordStream(String filename) throws Exception {
        OaiParser parser = initParser(filename, HeaderType.class, true);
        Iterator<Record> iterator = parser.iterator(Record.class);
        assertNotNull("missing record iterator", iterator);
        assertTrue("no record data", iterator.hasNext());
        Record next = iterator.next();
        assertNotNull("missing record", next);

        RecordTypeParser recordParser = next.getParser();
        assertNotNull("recordParser", recordParser);
        HeaderType parseHeader = recordParser.parseHeader();
        assertNotNull("parseHeader", parseHeader);
        // XXX
//            recordParser.parseMetadata(null);

//            assertFalse("unexpected record: " + next, iterator.hasNext());
        try {
            Record next2 = iterator.next();
            fail("unexpected record: " + next2);
        } catch (NoSuchElementException ex) {
            // it's expected
        }
    }

    @Test
    public void testListNoneRecord() throws Exception {
        String filename = "testListNoneRecord.xml";
        OaiParser parser = initParser(filename, HeaderType.class, false);
        Iterator<Record> iterator = parser.iterator(Record.class);
        assertNotNull("missing record iterator", iterator);
        assertFalse("unexpected hasNext", iterator.hasNext());
        try {
            Record next2 = iterator.next();
            fail("unexpected record: " + next2);
        } catch (NoSuchElementException ex) {
            // it's expected
        }
    }

    @Test
    public void testListRecordWithResumptionToken() throws Exception {
        String filename1 = "testListRecordWithResumptionToken.xml";
        String filename2 = "testListOneRecord.xml";
        OaiParser parser = initParser(filename1, HeaderType.class, false);
        Iterator<Record> iterator = parser.iterator(Record.class);
        assertNotNull("missing record iterator", iterator);
        assertTrue("no record data", iterator.hasNext());
        Record next = iterator.next();
        assertNotNull("missing record", next);
        assertFalse("unexpected record: " + next, iterator.hasNext());
        try {
            Record next2 = iterator.next();
            fail("unexpected record: " + next2);
        } catch (NoSuchElementException ex) {
            // it's expected
        }

        // token check
        ListRecordsType listRecords = parser.getOaiType().getListRecords();
        assertNotNull("listRecords", listRecords);
        ResumptionTokenType resumptionToken = listRecords.getResumptionToken();
        assertNotNull("resumptionToken", resumptionToken);
        String token = resumptionToken.getValue();
        assertEquals("token", "token", token);

        // read next file
        parser = initParser(filename2, HeaderType.class, false);
        iterator = parser.iterator(Record.class);
        assertNotNull("missing record iterator", iterator);
        assertTrue("no record data", iterator.hasNext());
        next = iterator.next();
        assertNotNull("missing record", next);
        assertFalse("unexpected record: " + next, iterator.hasNext());
        try {
            Record next2 = iterator.next();
            fail("unexpected record: " + next2);
        } catch (NoSuchElementException ex) {
            // it's expected
        }
        
        // test resumption token is null
        listRecords = parser.getOaiType().getListRecords();
        assertNotNull("listRecords", listRecords);
        resumptionToken = listRecords.getResumptionToken();
        assertNull("resumptionToken", resumptionToken);
    }

    @Test
    public void testResponseWithError() throws Exception {
        doTestResponseWithErrors("testResponseWithError.xml",
                new OAIPMHerrorType());
    }

    @Test
    public void testResponseWith2Errors() throws Exception {
        doTestResponseWithErrors("testResponseWith2Errors.xml",
                new OAIPMHerrorType(),
                new OAIPMHerrorType());
    }

    private void doTestResponseWithErrors(String filename, OAIPMHerrorType... expected) throws Exception {
        URL u = this.getClass().getResource(filename);
        assertNotNull("missing test file: " + filename, u);
        OaiParser parser = new OaiParser();
        parser.parse(u.openStream());
        OAIPMHtype oaiType = parser.getOaiType();
        assertNotNull("invalid oai data", oaiType);
        XMLGregorianCalendar responseDate = oaiType.getResponseDate();
        assertNotNull("invalid response date", responseDate);
        Iterator<Record> identifierIterator = parser.iterator(Record.class);
        assertNull("invalid iterator", identifierIterator);
        List<OAIPMHerrorType> errors = oaiType.getError();
        assertNotNull("null errors", errors);
        assertEquals(expected.length, errors.size());
//        assertArrayEquals("errors", expected, errors.toArray());
//
//        String errorStr = String.valueOf(error);
//        assertFalse("unexpected errors: " + error, error != null && !error.isEmpty());
    }

    @Test
    public void testBrokenXmlResponse() throws Exception {
        String filename = "testBrokenXmlResponse.xml";
        try {
            initParser(filename, HeaderType.class, false);
            fail("not catched broken OAI response");
        } catch (XMLStreamException ex) {
            // it's expected
        }
    }

    @Test
    public void testBrokenXmlListResponse() throws Exception {
        String filename = "testBrokenXmlListResponse.xml";
        OaiParser parser = initParser(filename, HeaderType.class, false);
        Iterator<Record> iterator = parser.iterator(Record.class);
        assertNotNull("missing record iterator", iterator);
        try {
            iterator.hasNext();
            fail("not catched broken OAI response");
        } catch (RuntimeException ex) {
            // it's expected
        }
    }

    private <T> OaiParser initParser(String filename, Class<T> invalidVerbType, boolean stream)
            throws IOException, JAXBException, XMLStreamException {

        URL u = this.getClass().getResource(filename);
        assertNotNull("missing test file: " + filename, u);
        OaiParser parser = new OaiParser(stream);
        parser.parse(u.openStream());
        OAIPMHtype oaiType = parser.getOaiType();
        assertNotNull("invalid oai data", oaiType);
        XMLGregorianCalendar responseDate = oaiType.getResponseDate();
        assertNotNull("invalid response date", responseDate);
        List<OAIPMHerrorType> error = oaiType.getError();
        assertFalse("unexpected errors: " + error, error != null && !error.isEmpty());
        Iterator<T> identifierIterator = parser.iterator(invalidVerbType);
        assertNull("invalid iterator for verb type: " + invalidVerbType, identifierIterator);
        return parser;
    }

    private static void makeLoggable(Class clazz, Level level) {
        Logger logger = Logger.getLogger(clazz.getName());
        logger.setLevel(level);
   }

}