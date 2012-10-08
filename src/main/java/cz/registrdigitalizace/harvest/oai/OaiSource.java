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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OAI source builds HTTP requests to harvest meta data.
 *
 * @author Jan Pokorsky
 * @see <a href='http://www.openarchives.org/OAI/openarchivesprotocol.html#HTTPRequestFormat'>OAI HTTP Request</a>
 */
public class OaiSource {

    private static final Logger LOG = Logger.getLogger(OaiSource.class.getName());
    private final String verbParameter;
    private final URI uri;

    protected OaiSource(URI uri, String verbParameter) {
        this.uri = uri;
        this.verbParameter = verbParameter;
    }

    /**
     * Gets initial HTTP request.
     * @return URL
     */
    public URL getUrl() {
        try {
            return uri.toURL();
        } catch (MalformedURLException ex) {
            // should not occur
            return null;
        }
    }

    /**
     * Gets subsequent HTTP request.
     * @param resumptionToken resumption token from previous responses.
     * @return URL
     */
    public URL getResumptionUrl(String resumptionToken) throws MalformedURLException {
        if (resumptionToken == null || resumptionToken.length() == 0) {
            throw new MalformedURLException("Invalid resumption token" + resumptionToken);
        }
        StringBuilder oaiQuery = new StringBuilder(verbParameter);
        oaiQuery.append("&resumptionToken=").append(resumptionToken);

        try {
            URI ru = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), oaiQuery.toString(), uri.getFragment());
            return ru.toURL();
        } catch (URISyntaxException ex) {
            MalformedURLException mex = new MalformedURLException(ex.getLocalizedMessage());
            mex.initCause(ex);
            throw mex;
        }
    }

    /**
     * Gets response from OAI source.
     * @param resumptionToken resumption token from previous responses.
     */
    public InputStream openConnection(String resumptionToken) throws IOException {
        URL url = getResumptionUrl(resumptionToken);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, url.toExternalForm());
        }
        return openStream(url);
    }

    /**
     * Gets initial response from OAI source.
     */
    public InputStream openConnection() throws IOException {
        URL url = getUrl();
        if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, url.toExternalForm());
        }
        return openStream(url);
    }

    private InputStream openStream(URL url) throws IOException {
        // XXX hadle HTTP response status
        // http://www.openarchives.org/OAI/openarchivesprotocol.html#HTTPResponseFormat
        // http://www.openarchives.org/OAI/2.0/guidelines-repository.htm#FlowControl
        // http://www.openarchives.org/OAI/2.0/guidelines-harvester.htm#FlowControl
        InputStream stream = url.openStream();
        return stream;
    }

}
