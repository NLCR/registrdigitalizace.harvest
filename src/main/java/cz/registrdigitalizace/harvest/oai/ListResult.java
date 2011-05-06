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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.openarchives.oai2.ListRecordsType;
import org.openarchives.oai2.OAIPMHerrorType;
import org.openarchives.oai2.OAIPMHtype;
import org.openarchives.oai2.ResumptionTokenType;

/**
 * Merges results of ListXXX queries into the only one iterator.
 *
 * <p><b>Since {@link Iterator} does not support checked exceptions it is necessary
 * to catch {@link RuntimeException} and check its cause for {@link OaiException}.</b></p>
 *
 * @author Jan Pokorsky
 */
public final class ListResult<T> extends OaiResult implements Iterable<T> {

    private final OaiParser parser;
    private final OaiSource source;
    private final Class<T> type;

    protected ListResult(OaiParser parser, OaiSource source, Class<T> type) {
        super(parser.getOaiType());
        this.parser = parser;
        this.source = source;
        this.type = type;
    }

    @Override
    public Iterator<T> iterator() {
        return new ResultIterator();
    }

    public void close() {
        this.parser.close();
    }

    private final class ResultIterator implements Iterator<T> {

        private Iterator<T> iterator;


        @Override
        public boolean hasNext() {
            if (iterator == null) {
                iterator = resolveIterator();
            }
            if (iterator == null) {
                return false;
            }
            boolean hasNext = iterator.hasNext();
            if (!hasNext) {
                fetchNext();
                iterator = resolveIterator();
                return iterator != null && iterator.hasNext();

            } else {
                return hasNext;
            }
        }

        /**
         * In case of resumption token exists it fetches further list from OAI source.
         */
        private void fetchNext() {
            String resumptionToken = getResumptionToken();
            if (resumptionToken == null) {
                return;
            }

            try {
                InputStream stream = source.openConnection(resumptionToken);
                parser.parse(stream);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            OAIPMHtype oaiType = parser.getOaiType();
            List<OAIPMHerrorType> errors = oaiType.getError();
            if (errors != null && !errors.isEmpty()) {
                URL url = null;
                try {
                    url = source.getResumptionUrl(resumptionToken);
                } catch (MalformedURLException ex) {
                    // ignore
                }
                throw new RuntimeException(new OaiException(errors, url));
            }
        }

        private String getResumptionToken() {
            OAIPMHtype oaiType = parser.getOaiType();
            ListRecordsType listRecords = oaiType.getListRecords();
            ResumptionTokenType resumptionToken = listRecords.getResumptionToken();
            return resumptionToken == null ? null : resumptionToken.getValue();
        }

        private Iterator<T> resolveIterator() {
            return parser.iterator(type);
        }

        @Override
        public T next() {
            if (hasNext()) {
                return iterator.next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

}
