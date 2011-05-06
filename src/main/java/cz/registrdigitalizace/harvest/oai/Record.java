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

import org.openarchives.oai2.RecordType;

/**
 * OAI record element. The usage depends on OaiParser mode. In case of
 * {@link OaiParser} runs with {@code listContentStreaming} set to {@code true} then
 * use {@link #getParser() } otherwise use {@link #getRecord() } and {@link #getXml() }.
 *
 * <p>XXX split to stand-alone classes Record and StreamedRecord?
 *
 * @author Jan Pokorsky
 */
public final class Record {

    private final RecordType record;
    private final String xml;
    private final RecordTypeParser parser;

    Record(RecordType record, String xml) {
        this(record, xml, null);
    }

    Record(RecordType record, String xml, RecordTypeParser parser) {
        this.record = record;
        this.xml = xml;
        this.parser = parser;
    }

    /**
     * Returns OAI record element.
     */
    public RecordType getRecord() {
        return record;
    }

    /**
     * Returns origin XML snippet containing OAI record element.
     */
    public String getXml() {
        return xml;
    }

    /**
     * Returns OAI record element parser.
     */
    public RecordTypeParser getParser() {
        return parser;
    }

    @Override
    public String toString() {
        String identifier = record == null ? null : record.getHeader().getIdentifier();
        return String.format("Record[id:%s, xml:%s, parser:%s]",
                identifier, xml, parser);
    }


}
