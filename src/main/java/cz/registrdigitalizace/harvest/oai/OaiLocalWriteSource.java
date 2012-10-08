/*
 * Copyright (C) 2012 Jan Pokorsky
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cz.registrdigitalizace.harvest.oai;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.logging.Logger;

/**
 * Wraps OAI source to dump harvest responses to local file system.
 *
 * <p>It creates {@code oai.xml} for first response and {@code oai_<resumptionToken>.xml}
 * for subsequent responses.
 *
 * @author Jan Pokorsky
 */
final class OaiLocalWriteSource extends OaiSource {

    private static final Logger LOG = Logger.getLogger(OaiLocalWriteSource.class.getName());
    private final OaiSource delegate;
    private final File folder;

    public OaiLocalWriteSource(OaiSource delegate, File folder) {
        super(null, null);
        if (!folder.exists() || !folder.isDirectory()) {
            throw new IllegalStateException("Invalid folder: " + folder);
        }
        String[] list = folder.list();
        if (list != null && list.length > 0) {
            throw new IllegalStateException("Folder not empty: " + folder);
        }
        this.delegate = delegate;
        this.folder = folder;
    }

    @Override
    public URL getResumptionUrl(String resumptionToken) throws MalformedURLException {
        return delegate.getResumptionUrl(resumptionToken);
    }

    @Override
    public URL getUrl() {
        return delegate.getUrl();
    }

    @Override
    public InputStream openConnection() throws IOException {
        InputStream input = delegate.openConnection();
        FileOutputStream fileOutputStream = new FileOutputStream(new File(folder, getHarvestFileName()));
        return new TeeInputStream(input, fileOutputStream);
    }

    @Override
    public InputStream openConnection(String resumptionToken) throws IOException {
        InputStream input = delegate.openConnection(resumptionToken);
        FileOutputStream fileOutputStream = new FileOutputStream(
                new File(folder, getResumptionFileName(resumptionToken)));
        return new TeeInputStream(input, fileOutputStream);
    }

    /**
     * Gets filename to persist first query
     */
    public static String getHarvestFileName() {
        return "oai.xml";
    }

    /**
     * Gets filename to persist next query
     */
    public static String getResumptionFileName(String resumptionToken) {
        try {
            String encodedResumptionToken = URLEncoder.encode(resumptionToken, "UTF-8");
            return "oai_" + encodedResumptionToken + ".xml";
        } catch (UnsupportedEncodingException ex) {
            // should not occur
            throw new IllegalStateException(resumptionToken, ex);
        }
    }

    /**
     * Makes copy of read data to particular output stream.
     */
    private static final class TeeInputStream extends InputStream {

        private final InputStream delegate;
        private final OutputStream out;

        public TeeInputStream(InputStream delegate, OutputStream out) {
            this.delegate = delegate;
            this.out = new BufferedOutputStream(out);
        }

        @Override
        public int read() throws IOException {
            int read = delegate.read();
            if (read != -1) {
                out.write(read);
            } else {
                out.flush();
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
            out.close();
        }

    }

}
