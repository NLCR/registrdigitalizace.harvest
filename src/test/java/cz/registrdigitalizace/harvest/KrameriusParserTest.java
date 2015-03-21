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
import cz.registrdigitalizace.harvest.metadata.ModsMetadataParser;
import java.util.Arrays;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import javax.xml.stream.XMLStreamReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static cz.registrdigitalizace.harvest.TestUtils.*;
import static org.junit.Assert.*;

/**
 *
 * @author Jan Pokorsky
 */
public class KrameriusParserTest {

    public KrameriusParserTest() {
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
    public void testParseKnihaZlata() throws Exception {
        String filename = "testParseKnihaZlata.xml";
        doTestParse(filename, 2);
    }

    @Test
    public void testParseDrobnustky() throws Exception {
        String filename = "testParseDrobnusky.xml";
        Metadata m = doTestParse(filename, 0);
        assertNotNull(m);
        assertSetEquals(asSet("Drobnůstky"), m.find(Metadata.NAZEV));
    }

    private Metadata doTestParse(String filename, int expectedRelations) throws Exception {
        XmlContext xmlContext = new XmlContext();
        InputStream input = KrameriusParserTest.class.getResourceAsStream(filename);
        assertNotNull("missing: " + filename, input);
        XMLStreamReader reader = xmlContext.createStreamParser(input, null);

        KrameriusParser parser = new KrameriusParser(xmlContext, new ModsMetadataParser(ModsMetadataParser.STYLESHEET));
        HarvestedRecord record = parser.parse(reader);
        assertNotNull(record);

//        System.out.printf("record: uuid: %s, type: %s, root: %s\n",
//                record.getUuid(), record.getType(), record.isRoot(), record.getDescriptor());
        assertTrue(record.isRoot());
        assertEquals(expectedRelations, record.getChildren().size());
        return record.getMetadata();
    }

    @Test
    public void testParseOneRecord() throws Exception {
        String filename = "testKrameriusParseRootEmptyRecord.xml";
        XmlContext xmlContext = new XmlContext();
        InputStream input = KrameriusParserTest.class.getResourceAsStream(filename);
        assertNotNull("missing: " + filename, input);
        XMLStreamReader reader = xmlContext.createStreamParser(input, null);

        KrameriusParser parser = new KrameriusParser(xmlContext, new ModsMetadataParser(ModsMetadataParser.STYLESHEET));
        HarvestedRecord record = parser.parse(reader);
        assertNotNull("HarvestedRecord", record);
        assertValidRecord(true, "cd2b2ad0-62d4-11dd-ac0e-000d606f5dc6", "MONOGRAPH", Collections.<String>emptyList(), record);
    }

    @Test
    public void testFullParseRecordTree() throws Exception {
        String filename = "testKrameriusParseRecordWithRelations.xml";
        XmlContext xmlContext = new XmlContext();
        InputStream input = KrameriusParserTest.class.getResourceAsStream(filename);
        assertNotNull("missing: " + filename, input);
        XMLStreamReader reader = xmlContext.createStreamParser(input, null);

        KrameriusParser parser = new KrameriusParser(xmlContext, new ModsMetadataParser(ModsMetadataParser.STYLESHEET));
        HarvestedRecord record = parser.parse(reader);
        assertNotNull("HarvestedRecord", record);
        assertValidRecord(false, "cd2b2ad0-62d4-11dd-ac0e-000d606f5dc6", "PERIODICAL_VOLUME",
                Arrays.asList("1", "2"), record);
    }

    private void assertValidRecord(boolean expectedRoot, String expectedUuid, String expectedType, List<String> expectedChildren, HarvestedRecord record) {
        assertNotNull("record: " + record, record);
        assertNull("getId: " + record, record.getId());
        assertEquals("getUuid: ", expectedUuid, record.getUuid());
        assertEquals("getType: " + record, expectedType, record.getType());
        assertNotNull("xml", record.getDescriptor());
        assertEquals("isRoot: " + record, expectedRoot, record.isRoot());
        assertEquals("children", expectedChildren, record.getChildren());
    }

}