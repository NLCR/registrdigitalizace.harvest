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

package cz.registrdigitalizace.harvest.db;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * @author Jan Pokorsky
 */
public final class ThumbnailDao {

    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    public void insert(BigDecimal digiObjId, String mimeType, byte[] contents) throws DaoException {
        try {
            doInsert(digiObjId, mimeType, contents);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    public void insert(BigDecimal digiObjId, String mimeType, InputStream contents, int length) throws DaoException {
        try {
            doInsert(digiObjId, mimeType, contents, length);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    /**
     * Returns an iterator over DIGOBJEKTS whose THUMBNAILS should be downloaded.
     * The result contains <b>ALL</b> locations of DIGOBJEKT.
     */
    public IterableResult findMissing() throws DaoException {
        try {
            return doFindMissing();
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    /**
     * Deletes all THUMBNAILS whose DIGOBJEKTS were removed at the last harvest.
     */
    public void deleteUnrelated() throws DaoException {
        try {
            doDeleteUnrelated();
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doDeleteUnrelated() throws SQLException {
        Connection conn = source.getConnection();
        Statement stmt = conn.createStatement();
        try {
            stmt.executeUpdate("delete from THUMBNAILS where DIGOBJEKTID not in (select ID from DIGOBJEKT)");
        } finally {
            SQLQuery.tryClose(stmt);
        }
    }
    
    private IterableResult doFindMissing() throws SQLException {
        Connection conn = source.getConnection();
        Statement stmt = conn.createStatement();
        boolean failure = true;
        try {
            ResultSet rs = stmt.executeQuery(SQLQuery.getFindMissingThumbnails());
            failure = false;
            return new IterableResult(rs, stmt);
        } finally {
            if (failure) {
                SQLQuery.tryClose(stmt);
            }
        }
    }

    private void doInsert(BigDecimal digiObjId, String mimeType, InputStream contents, int length) throws SQLException {
        Connection conn = source.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(
                "insert into THUMBNAILS (DIGOBJEKTID, MIME, CONTENTS) values (?, ?, ?)");
        try {
            pstmt.setBigDecimal(1, digiObjId);
            pstmt.setString(2, mimeType);
//            pstmt.setBlob(3, contents, length);
            pstmt.setBinaryStream(3, contents, length);
            pstmt.executeUpdate();
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    private void doInsert(BigDecimal digiObjId, String mimeType, byte[] contents) throws SQLException {
        Connection conn = source.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(
                "insert into THUMBNAILS (DIGOBJEKTID, MIME, CONTENTS) values (?, ?, ?)");
        try {
            pstmt.setBigDecimal(1, digiObjId);
            pstmt.setString(2, mimeType);
            pstmt.setBytes(3, contents);
            pstmt.executeUpdate();
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    public static final class Thumbnail {
        private BigDecimal digiObjId;
        private BigDecimal libraryId;
        private String uuid;

        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public BigDecimal getDigiObjId() {
            return digiObjId;
        }

        public void setDigiObjId(BigDecimal digiObjId) {
            this.digiObjId = digiObjId;
        }

        public BigDecimal getLibraryId() {
            return libraryId;
        }

        public void setLibraryId(BigDecimal libraryId) {
            this.libraryId = libraryId;
        }
    }

    public static final class IterableResult implements Iterable<Thumbnail>, Iterator<Thumbnail> {

        private ResultSet rs;
        private Statement stmt;
        private boolean lastNext = false;

        public IterableResult(ResultSet rs, Statement stmt) {
            this.rs = rs;
            this.stmt = stmt;
        }

        @Override
        public Iterator<Thumbnail> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            try {
                return hasNextResult();
            } catch (DaoException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public Thumbnail next() {
            try {
                return nextResult();
            } catch (DaoException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported.");
        }

        public boolean hasNextResult() throws DaoException {
            try {
                lastNext = rs.next();
                return lastNext;
            } catch (SQLException ex) {
                throw new DaoException(ex);
            }
        }

        public Thumbnail nextResult() throws DaoException, NoSuchElementException {
            try {
                if (!lastNext && !hasNextResult()) {
                    throw new NoSuchElementException();
                }
                Thumbnail thumbnail = new Thumbnail();
                thumbnail.setDigiObjId(rs.getBigDecimal("RDIGOBJEKTL"));
                thumbnail.setUuid(rs.getString("UUID"));
                thumbnail.setLibraryId(rs.getBigDecimal("LIBID"));
                return thumbnail;
            } catch (SQLException ex) {
                throw new DaoException(ex);
            }
        }

        public void close() {
            SQLQuery.tryClose(rs);
            SQLQuery.tryClose(stmt);
        }

    }
}
