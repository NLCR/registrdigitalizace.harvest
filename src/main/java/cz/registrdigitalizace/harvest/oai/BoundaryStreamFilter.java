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

import javax.xml.namespace.QName;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

/**
 * Filter to define a boundaries of the {@link BoundaryStreamReader }.
 *
 * @author Jan Pokorsky
 */
public class BoundaryStreamFilter implements StreamFilter {
    
    private State state = State.INIT;
    private QName boundary;

    public BoundaryStreamFilter(QName boundary) {
        this.boundary = boundary;
    }


    enum State {INIT, SEARCHING, FINISHED}

    @Override
    public boolean accept(XMLStreamReader reader) {
        switch(state) {
            case INIT:
                if (reader.getEventType() == XMLEvent.START_ELEMENT) {
                    if (!boundary.equals(reader.getName())) {
                        throw new IllegalStateException("Expected: " + boundary + ", found: " + reader.getName());
                    }
                    boundary = reader.getName();
                    state = State.SEARCHING;
                    return true;
                } else {
                    throw new IllegalStateException("Cannot initialize boundary begining.");
                }
            case SEARCHING:
                if (reader.getEventType() == XMLEvent.END_ELEMENT) {
                    if (boundary.equals(reader.getName())) {
                        state = State.FINISHED;
                        return false;
                    }
                }
                return true;
            case FINISHED:
                return false;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("BoundaryStreamFilter[%s, %s]", boundary, state);
    }


}
