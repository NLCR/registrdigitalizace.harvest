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

import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;

/**
 * Filtered XML stream reader to read just bounded part of the underlying stream.
 *
 * @author Jan Pokorsky
 */
public class BoundaryStreamReader extends StreamReaderDelegate{

    private boolean stop;
    private StreamFilter filter;
    public static boolean debug = false;

    public BoundaryStreamReader(XMLStreamReader reader, StreamFilter filter) {
        super(reader);
        this.stop = !filter.accept(reader);
        this.filter = filter;
    }

    @Override
    public boolean hasNext() throws XMLStreamException {
        if (stop) {
            return false;
        }

        return getParent().hasNext();
    }

    @Override
    public int next() throws XMLStreamException {
        if (stop || !hasNext()) {
            throw new XMLStreamException(null, getLocation());
        }

        int next = getParent().next();
        stop = !filter.accept(this);
        return next;
    }

    @Override
    public int nextTag() throws XMLStreamException {
        if (stop) {
            throw new XMLStreamException(null, getLocation());
        }

        int next = getParent().nextTag();
        stop = !filter.accept(this);
        return next;
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", getClass().getSimpleName(), filter);
    }

}
