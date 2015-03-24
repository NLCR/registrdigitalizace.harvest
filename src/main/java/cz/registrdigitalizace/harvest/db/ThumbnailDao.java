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

/**
 *
 * @author Jan Pokorsky
 */
public final class ThumbnailDao {

    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    public void insert(BigDecimal libraryId, String digiObjUuid, String thumbFilename,
            String mimeType, byte[] contents) throws DaoException {
        try {
            doInsert(libraryId, digiObjUuid, thumbFilename, mimeType, contents);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    public void insert(BigDecimal libraryId, String digiObjUuid, String thumbFilename,
            String mimeType, InputStream contents, int length) throws DaoException {
        try {
            doInsert(libraryId, digiObjUuid, thumbFilename, mimeType, contents, length);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    /**
     * Returns an iterator over DIGOBJEKTS whose THUMBNAILS should be downloaded.
     * The result contains <b>ALL</b> locations of DIGOBJEKT.
     */
    public IterableResult<Thumbnail> findMissing() throws DaoException {
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
            stmt.executeUpdate("delete from THUMBNAILS where "
                    + "(DIGOBJEKTUUID,DIGKNIHOVNA) not in (select UUID,RDIGKNIHOVNA_DIGOBJEKT from DIGOBJEKT)");
        } finally {
            SQLQuery.tryClose(stmt);
        }
    }
    
    private IterableResult<Thumbnail> doFindMissing() throws SQLException {
        Connection conn = source.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("select ID, UUID, RDIGKNIHOVNA_DIGOBJEKT as LIBID from DIGOBJEKT"
                    + " where (UUID,RDIGKNIHOVNA_DIGOBJEKT) not in (select DIGOBJEKTUUID,DIGKNIHOVNA from THUMBNAILS)");
            return new ThumbnailResult(rs, stmt);
        } finally {
            if (rs == null) {
                SQLQuery.tryClose(stmt);
            }
        }
    }

    private void doInsert(BigDecimal libraryId, String digiObjUuid, String thumbFilename,
            String mimeType, InputStream contents, int length) throws SQLException {
        Connection conn = source.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(
                "insert into THUMBNAILS (DIGKNIHOVNA, DIGOBJEKTUUID, TNFILENAME, MIME, CONTENTS) values (?, ?, ?, ?, ?)");
        try {
            int col = 1;
            pstmt.setBigDecimal(col++, libraryId);
            pstmt.setString(col++, digiObjUuid);
            pstmt.setString(col++, thumbFilename);
            pstmt.setString(col++, mimeType);
//            pstmt.setBlob(3, contents, length);
            pstmt.setBinaryStream(col++, contents, length);
            pstmt.executeUpdate();
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    private void doInsert(BigDecimal libraryId, String digiObjUuid, String thumbFilename,
            String mimeType, byte[] contents) throws SQLException {
        Connection conn = source.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(
                "insert into THUMBNAILS (DIGKNIHOVNA, DIGOBJEKTUUID, TNFILENAME, MIME, CONTENTS) values (?, ?, ?, ?, ?)");
        try {
            int col = 1;
            pstmt.setBigDecimal(col++, libraryId);
            pstmt.setString(col++, digiObjUuid);
            pstmt.setString(col++, thumbFilename);
            pstmt.setString(col++, mimeType);
            pstmt.setBytes(col++, contents);
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

    private static final class ThumbnailResult extends IterableResult<Thumbnail> {

        public ThumbnailResult(ResultSet rs, Statement stmt) {
            super(stmt, rs);
        }

        @Override
        protected Thumbnail fetchNext() throws DaoException {
            try {
                Thumbnail thumbnail = new Thumbnail();
                thumbnail.setDigiObjId(rs.getBigDecimal("ID"));
                thumbnail.setUuid(rs.getString("UUID"));
                thumbnail.setLibraryId(rs.getBigDecimal("LIBID"));
                return thumbnail;
            } catch (SQLException ex) {
                throw new DaoException(ex);
            }
        }
    }
}
