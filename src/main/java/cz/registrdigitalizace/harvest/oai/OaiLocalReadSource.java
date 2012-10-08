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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;

/**
 * Wraps OAI source to read locally cached harvest responses.
 *
 * <p>It expects format of {@link OaiLocalWriteSource}.
 *
 * @author Jan Pokorsky
 */
final class OaiLocalReadSource extends OaiSource {

    private static final Logger LOG = Logger.getLogger(OaiLocalReadSource.class.getName());
    private final File folder;

    public OaiLocalReadSource(File folder) {
        super(null, null);
        if (!folder.exists() || !folder.isDirectory()) {
            throw new IllegalStateException("Invalid folder: " + folder);
        }
        this.folder = folder;
    }

    @Override
    public URL getResumptionUrl(String resumptionToken) throws MalformedURLException {
            File file = new File(folder, OaiLocalWriteSource.getResumptionFileName(resumptionToken));
            return file.toURI().toURL();
    }

    @Override
    public URL getUrl() {
        File file = new File(folder, OaiLocalWriteSource.getHarvestFileName());
        try {
            return file.toURI().toURL();
        } catch (MalformedURLException ex) {
            throw new IllegalStateException(ex);
        }
    }

}
