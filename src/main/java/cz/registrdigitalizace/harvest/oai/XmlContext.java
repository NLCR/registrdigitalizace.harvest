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
import java.io.InputStream;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.XMLEventAllocator;
import org.openarchives.oai2.ObjectFactory;

/**
 *
 * @author Jan Pokorsky
 */
public final class XmlContext {

    private Unmarshaller unmarshaller;
    private JAXBContext ctx;
    private XMLInputFactory xmlInputFactory;
    private XMLOutputFactory xmlOutputFactory;
    private XMLEventFactory xmlEventFactory;
    private XMLEventAllocator xmlEventAllocator;

    public Unmarshaller getUnmarshaller() throws JAXBException {
        if (unmarshaller == null) {
            initContext();
            unmarshaller = ctx.createUnmarshaller();
        }
        return unmarshaller;
    }

    public XMLInputFactory getXMLInputFactory() {
        if (xmlInputFactory == null) {
            xmlInputFactory = XMLInputFactory.newFactory();
        }
        return xmlInputFactory;
    }

    public XMLOutputFactory getXMLOutputFactory() {
        if (xmlOutputFactory == null) {
            xmlOutputFactory = XMLOutputFactory.newFactory();
        }
        return xmlOutputFactory;
    }

    public XMLEventFactory getXMLEventFactory() {
        if (xmlEventFactory == null) {
            xmlEventFactory = XMLEventFactory.newFactory();
        }
        return xmlEventFactory;
    }

    public XMLEventAllocator getXMLEventAllocator() throws IOException {
        if (xmlEventAllocator == null) {
            xmlEventAllocator = loadAllocator();
        }
        return xmlEventAllocator;
    }

    public XMLStreamReader createStreamParser(InputStream stream, StreamFilter filter) throws XMLStreamException {
        final XMLInputFactory inf = getXMLInputFactory();
        XMLStreamReader reader;

        reader = inf.createXMLStreamReader(stream);
        if (filter != null) {
            reader = inf.createFilteredReader(reader, filter);
        }
        return reader;
    }

    private void initContext() throws JAXBException {
        if (ctx == null) {
            ctx = JAXBContext.newInstance(ObjectFactory.class);
        }
    }

    public static XMLEventAllocator loadAllocator() throws IOException {
        try {
            Class<?> loadClass = XmlContext.class.getClassLoader().
                    loadClass("com.sun.xml.internal.stream.events.XMLEventAllocatorImpl");
            Object newInstance = loadClass.newInstance();
            return (XMLEventAllocator) newInstance;
        } catch (InstantiationException ex) {
            throw new IOException(ex);
        } catch (IllegalAccessException ex) {
            throw new IOException(ex);
        } catch (ClassNotFoundException ex) {
            throw new IOException(ex);
        }
    }

}
