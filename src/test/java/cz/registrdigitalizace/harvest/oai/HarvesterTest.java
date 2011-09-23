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

import cz.registrdigitalizace.harvest.KrameriusParser;
import cz.registrdigitalizace.harvest.HarvestedRecord;
import cz.registrdigitalizace.harvest.metadata.ModsMetadataParser;
import java.net.URL;
import java.util.Iterator;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.openarchives.oai2.HeaderType;

/**
 *
 * @author Jan Pokorsky
 */
public class HarvesterTest {

    public HarvesterTest() {
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
    public void testGetListRecords() throws Exception {
        doTestGetListRecords(false);
    }

    @Test
    public void testGetListRecordsStreamed() throws Exception {
        doTestGetListRecords(true);
    }

    private void doTestGetListRecords(boolean listContentStreaming) throws Exception {
//        String filename = "testListOneRecord.xml";
        String filename = "testListOneRecordDrKram.xml";
        URL u = this.getClass().getResource(filename);
        assertNotNull("missing test file: " + filename, u);

        XmlContext xmlContext = new XmlContext();
        OaiSource mockedSource = EasyMock.createMock(OaiSource.class);
        EasyMock.expect(mockedSource.openConnection()).andReturn(u.openStream());

        EasyMock.replay(mockedSource);

        Harvester dao = new Harvester(mockedSource, xmlContext);
        // verify result
        ListResult<Record> listRecords = dao.getListRecords(listContentStreaming);
        try {
            assertNotNull("listRecords", listRecords);
            Iterator<Record> iterator = listRecords.iterator();
            assertNotNull("iterator", iterator);
            assertTrue("hasNext", iterator.hasNext());
            Record next = iterator.next();
            assertNotNull("next", next);
            if (listContentStreaming) {
                RecordTypeParser parser = next.getParser();
                assertNotNull(parser);
                HeaderType header = parser.parseHeader();
                assertNotNull(header);
                HarvestedRecord parsedMetadata = parser.parseMetadata(new KrameriusParser(xmlContext, new ModsMetadataParser(ModsMetadataParser.MZK_STYLESHEET)));
                assertNotNull(parsedMetadata);
            }
            assertFalse("hasNext2", iterator.hasNext());
        } finally {
            listRecords.close();
        }

        EasyMock.verify(mockedSource);
    }

    @Test
    public void testGet2ListRecords() throws Exception {
        doTestGet2ListRecords(false);
    }

    @Test
    public void testGet2ListRecordsStreamed() throws Exception {
        doTestGet2ListRecords(true);
    }

    private void doTestGet2ListRecords(boolean listContentStreaming) throws Exception {
        String filename1 = "testListRecordWithResumptionToken.xml";
        String filename2 = "testListOneRecord.xml";
        URL u1 = this.getClass().getResource(filename1);
        assertNotNull("missing test file: " + filename1, u1);
        URL u2 = this.getClass().getResource(filename2);
        assertNotNull("missing test file: " + filename2, u2);

        XmlContext xmlContext = new XmlContext();
        OaiSource mockedSource = EasyMock.createMock(OaiSource.class);
        EasyMock.expect(mockedSource.openConnection()).andReturn(u1.openStream());
        EasyMock.expect(mockedSource.openConnection(EasyMock.eq("token"))).andReturn(u2.openStream());

        EasyMock.replay(mockedSource);

        Harvester dao = new Harvester(mockedSource, xmlContext);
        // verify result
        ListResult<Record> listRecords = dao.getListRecords(listContentStreaming);
        try {
            assertNotNull("listRecords", listRecords);
            Iterator<Record> iterator = listRecords.iterator();
            assertNotNull("iterator", iterator);
            assertTrue("hasNext1", iterator.hasNext());
            assertNotNull("next1", iterator.next());
            assertTrue("hasNext2", iterator.hasNext());
            assertNotNull("next2", iterator.next());
            assertFalse("hasNext3", iterator.hasNext());
        } finally {
            listRecords.close();
        }

        EasyMock.verify(mockedSource);
    }

    @Test
    public void testGetListRecordsWithOaiError() throws Exception {
        String filename = "testResponseWithError.xml";
        URL u = this.getClass().getResource(filename);
        assertNotNull("missing test file: " + filename, u);

        XmlContext xmlContext = new XmlContext();
        OaiSource mockedSource = EasyMock.createMock(OaiSource.class);
        EasyMock.expect(mockedSource.openConnection()).andReturn(u.openStream());
        EasyMock.expect(mockedSource.getUrl()).andReturn(u);

        EasyMock.replay(mockedSource);

        Harvester dao = new Harvester(mockedSource, xmlContext);
        // verify result
        try {
            ListResult<Record> listRecords = dao.getListRecords(false);
            fail("unexpected list records");
        } catch (OaiException ex) {
            // expected
            assertEquals(1, ex.getErrors().size());
        }

        EasyMock.verify(mockedSource);
    }

    @Test
    public void testGet2ListRecordsWithOaiErrors() throws Exception {
        String filename1 = "testListRecordWithResumptionToken.xml";
        String filename2 = "testResponseWith2Errors.xml";
        URL u1 = this.getClass().getResource(filename1);
        assertNotNull("missing test file: " + filename1, u1);
        URL u2 = this.getClass().getResource(filename2);
        assertNotNull("missing test file: " + filename2, u2);

        XmlContext xmlContext = new XmlContext();
        OaiSource mockedSource = EasyMock.createMock(OaiSource.class);
        EasyMock.expect(mockedSource.openConnection()).andReturn(u1.openStream());
        EasyMock.expect(mockedSource.openConnection(EasyMock.eq("token"))).andReturn(u2.openStream());
        EasyMock.expect(mockedSource.getResumptionUrl(EasyMock.eq("token"))).andReturn(u2);

        EasyMock.replay(mockedSource);

        Harvester dao = new Harvester(mockedSource, xmlContext);
        // verify result
        ListResult<Record> listRecords = dao.getListRecords(true);
        try {
            assertNotNull("listRecords", listRecords);
            Iterator<Record> iterator = listRecords.iterator();
            assertNotNull("iterator", iterator);
            assertTrue("hasNext1", iterator.hasNext());
            assertNotNull("next1", iterator.next());
            try {
                iterator.hasNext();
                fail("unexpected hasNext");
            } catch (RuntimeException rex) {
                Throwable cause = rex.getCause();
                assertEquals(OaiException.class, cause.getClass());
                assertEquals(2, ((OaiException) cause).getErrors().size());
            }
        } finally {
            listRecords.close();
        }

        EasyMock.verify(mockedSource);
    }

}
