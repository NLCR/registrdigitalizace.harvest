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

import java.io.InputStream;
import java.io.StringReader;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Jan Pokorsky
 */
public class BoundaryStreamReaderTest {
    private InputStream is;

    public BoundaryStreamReaderTest() {
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
    public void testBoundaryStream() throws Exception {
        String xml = "<?xml version='1.0' ?><a><b></b></a>";
        QName aName = new QName("a");
        QName bName = new QName("b");
        XmlContext xmlContext = new XmlContext();
        XMLStreamReader reader = xmlContext.getXMLInputFactory().createXMLStreamReader(new StringReader(xml));
        assertEquals(XMLEvent.START_DOCUMENT, reader.getEventType());
        assertTrue(reader.hasNext());
        assertEquals(XMLEvent.START_ELEMENT, reader.next());
        XMLStreamReader bsreader = new BoundaryStreamReader(reader, new BoundaryStreamFilter(aName));
        assertEquals("is <a>", XMLEvent.START_ELEMENT, bsreader.getEventType());
        assertTrue("has <b>", bsreader.hasNext());
        assertEquals("is <b>", XMLEvent.START_ELEMENT, bsreader.next());
        assertEquals(bName, bsreader.getName());

        assertTrue("has </b>", bsreader.hasNext());
        assertEquals("is </b>", XMLEvent.END_ELEMENT, bsreader.next());
        assertEquals(bName, bsreader.getName());

        assertTrue("has </a>", bsreader.hasNext());
        assertEquals("is </a>", XMLEvent.END_ELEMENT, bsreader.next());
        assertEquals(aName, bsreader.getName());

        assertFalse("end bsreader", bsreader.hasNext());
        assertEquals("is </a>", XMLEvent.END_ELEMENT, reader.getEventType());
        assertEquals(aName, reader.getName());
        assertTrue("end reader", reader.hasNext());
        assertEquals("is end document", XMLEvent.END_DOCUMENT, reader.next());
    }

    @Test
    public void testNestedBoundaryStreams() throws Exception {
        String xml = "<?xml version='1.0' ?><a><b><c></c></b></a>";
        QName aName = new QName("a");
        QName bName = new QName("b");
        QName cName = new QName("c");
        XmlContext xmlContext = new XmlContext();
        XMLStreamReader reader = xmlContext.getXMLInputFactory().createXMLStreamReader(new StringReader(xml));
        assertEquals(XMLEvent.START_DOCUMENT, reader.getEventType());
        assertTrue(reader.hasNext());
        assertEquals(XMLEvent.START_ELEMENT, reader.next());
        XMLStreamReader bsreaderA = new BoundaryStreamReader(reader, new BoundaryStreamFilter(aName));
        assertEquals("is <a>", XMLEvent.START_ELEMENT, bsreaderA.getEventType());
        assertTrue("has <b>", bsreaderA.hasNext());
        assertEquals("is <b>", XMLEvent.START_ELEMENT, bsreaderA.next());
        assertEquals(bName, bsreaderA.getName());

        // begin B reader
        XMLStreamReader bsreaderB = new BoundaryStreamReader(bsreaderA, new BoundaryStreamFilter(bName));
        assertEquals("is <b>", XMLEvent.START_ELEMENT, bsreaderB.getEventType());
        assertEquals(bName, bsreaderB.getName());
        assertTrue("has <c>", bsreaderB.hasNext());
        assertEquals("is <c>", XMLEvent.START_ELEMENT, bsreaderB.next());
        assertEquals(cName, bsreaderB.getName());

        assertTrue("has </c>", bsreaderB.hasNext());
        assertEquals("is </c>", XMLEvent.END_ELEMENT, bsreaderB.next());
        assertEquals(cName, bsreaderB.getName());

        assertTrue("has </b>", bsreaderB.hasNext());
        assertEquals("is </b>", XMLEvent.END_ELEMENT, bsreaderB.next());
        assertEquals(bName, bsreaderB.getName());
        assertFalse("end bsreaderB", bsreaderB.hasNext());
        // end of B reader

        assertEquals("is </b>", XMLEvent.END_ELEMENT, bsreaderA.getEventType());
        assertEquals(bName, bsreaderA.getName());
        assertTrue("has </a>", bsreaderA.hasNext());
        assertEquals("is </a>", XMLEvent.END_ELEMENT, bsreaderA.next());
        assertEquals(aName, bsreaderA.getName());
        assertFalse("end bsreaderA", bsreaderA.hasNext());

        assertEquals("is </a>", XMLEvent.END_ELEMENT, reader.getEventType());
        assertEquals(aName, reader.getName());
        
        assertTrue("hasNext end document", reader.hasNext());
        assertEquals("is end document", XMLEvent.END_DOCUMENT, reader.next());
        assertFalse("end reader", reader.hasNext());
    }


}