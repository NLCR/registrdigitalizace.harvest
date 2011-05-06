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

import cz.registrdigitalizace.harvest.db.DaoException;
import cz.registrdigitalizace.harvest.db.DigitizationRegistrySource;
import cz.registrdigitalizace.harvest.db.HarvestTransaction;
import cz.registrdigitalizace.harvest.db.Library;
import cz.registrdigitalizace.harvest.db.ThumbnailDao;
import cz.registrdigitalizace.harvest.db.ThumbnailDao.IterableResult;
import cz.registrdigitalizace.harvest.db.ThumbnailDao.Thumbnail;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Downloads missing thumbnails of digitized objects and cleans up thumbnails
 * of all removed objects.
 *
 * @author Jan Pokorsky
 */
public final class ThumbnailHarvest {
    private static final Logger LOG = Logger.getLogger(ThumbnailHarvest.class.getName());
    private static final int MAX_THUMBNAIL_SIZE = 10 * 1024 * 1024; // 10 MB
    
    private final DigitizationRegistrySource dataSource;
    private final List<Library> libraries;
    private int counter;
    private long sizeCounter;
    private final ShareableBuffer bufferedThumbnail = new ShareableBuffer(50 * 1024);
    private final byte[] buffer = new byte[20 * 1024];
//    private FileOutputStream dump;

    public ThumbnailHarvest(DigitizationRegistrySource dataSource, List<Library> libraries) {
        this.dataSource = dataSource;
        this.libraries = libraries;
    }

    public void harvestThumbnails() throws DaoException, IOException {
//        dump = new FileOutputStream("/tmp/mzk-urls.txt");
        HarvestTransaction transaction = new HarvestTransaction(dataSource);
        ThumbnailDao thumbnailDao = new ThumbnailDao();
        thumbnailDao.setDataSource(transaction);
        boolean rollback = true;
        try {
            transaction.begin();
            thumbnailDao.deleteUnrelated();

            IterableResult missings = thumbnailDao.findMissing();
            try {
                Thumbnail last = null;
                while (missings.hasNextResult()) {
                    Thumbnail thumbnail = missings.nextResult();
                    if (last != null && last.getDigiObjId().equals(thumbnail.getDigiObjId())) {
                        continue;
                    }
                    Library lib = findLibrary(thumbnail.getLibraryId());
                    URL url = thumbnailUrl(lib, thumbnail);
                    boolean ok = downloadThumbnail(url, thumbnail, thumbnailDao);
                    if (ok) {
                        last = thumbnail;
                        if (counter % 200 == 0) {
                            transaction.commit();
                        }
                    }
//                    if (counter > 1000) {
//                        break;
//                    }
                }
            } finally {
                missings.close();
            }

            transaction.commit();
            rollback = false;
        } finally {
//            dump.close();
            if (rollback) {
                transaction.rollback();
            }
            transaction.close();
        }
    }

    public int getTotalNumber() {
        return counter;
    }

    public long getTotalSize() {
        return sizeCounter;
    }

    private Library findLibrary(BigDecimal libId) {
        for (Library library : libraries) {
            if (libId.equals(library.getId())) {
                return library;
            }
        }
        throw new IllegalStateException("Library not found: " + libId);
    }

    private URL thumbnailUrl(Library lib, Thumbnail t) throws IOException {
        try {
            URI uri = URI.create(lib.getBaseUrl());
            uri = new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(),
                    uri.getPort(),
                    "/search/img", "uuid=" + t.getUuid() + "&stream=IMG_THUMB&action=GETRAW",
                    null);
            URL url = uri.toURL();
//            dump.write(url.toExternalForm().getBytes());
//            dump.write('\n');
            return url;
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }

    private boolean downloadThumbnail(URL url, Thumbnail thumbnail, ThumbnailDao thumbnailDao) throws DaoException, IOException {
        String contentType = null;
        String contentEncoding = null;
        Integer contentLength = null;
        Integer responseCode = null;
        ByteArrayInputStream thumbnailSnapshot = null;
        String errMsg = null;
        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            responseCode = conn.getResponseCode();
            contentType = conn.getContentType();
            contentLength = conn.getContentLength();
            contentEncoding = conn.getContentEncoding();
            if (contentLength < 0) {
                LOG.log(Level.WARNING, "{0} => response: {1}, length: {2}, type: {3}",
                        new Object[]{url.toString(), responseCode, contentLength, contentType});
            }
            errMsg = dumpErrorStream(conn.getErrorStream(), contentEncoding);
            thumbnailSnapshot = makeSnapshot(conn.getInputStream());
        } catch (IOException ex) {
            String msg = String.format(
                    "%s\nresponseCode: %s, contentLength: %s, contentType: %s,"
                        + " contentEncoding: %s, ErrorStream.content:\n%s\n",
                    url, responseCode, contentLength, contentType,
                    contentEncoding, errMsg);
            LOG.log(Level.SEVERE, msg, ex);
            return false;
        }
        try {
            // use real length
            contentLength = thumbnailSnapshot.available(); // XXX not reliable
            thumbnailDao.insert(thumbnail.getDigiObjId(), contentType, thumbnailSnapshot, contentLength);
            sizeCounter += contentLength;
            counter++;
            return true;
        } finally {
            thumbnailSnapshot.close();
        }
    }

    private String dumpErrorStream(InputStream is, String encoding) {
        if (is == null) {
            return "";
        }
        Charset charset = Charset.defaultCharset();
        try {
            if (encoding == null) {
                charset = Charset.forName(encoding);
            }
        } catch (Exception ex) {
            // ignore
        }
        try {
            try {
                // hack
                readThumbnail(is);
                return new String(bufferedThumbnail.getBuf(), 0, bufferedThumbnail.size(), charset);
            } finally {
                is.close();
            }
        } catch (IOException ex) {
            // ignore
        }
        return "";
    }

    private ByteArrayInputStream makeSnapshot(InputStream inputStream) throws IOException {
        try {
            ByteArrayInputStream thumbnailSnapshot = readThumbnail(inputStream);
            return thumbnailSnapshot;
        } finally {
            inputStream.close();
        }
    }

    private ByteArrayInputStream readThumbnail(InputStream is) throws IOException {
        LOG.log(Level.FINEST, "available: {0}", is.available());
        bufferedThumbnail.reset();
        for (int length; (length = is.read(buffer)) > 0;) {
            if (length > MAX_THUMBNAIL_SIZE) {
                throw new IOException("Too big to be a thumbnail.");
            }
            bufferedThumbnail.write(buffer, 0, length);
        }
        return new ByteArrayInputStream(bufferedThumbnail.getBuf(), 0, bufferedThumbnail.size());
    }

    /**
     * Helper class to prevent making buffer copy in {@link #toByteArray() toByteArray()}.
     */
    private static final class ShareableBuffer extends ByteArrayOutputStream {

        public ShareableBuffer(int size) {
            super(size);
        }

        public byte[] getBuf() {
            return buf;
        }

    }

}
