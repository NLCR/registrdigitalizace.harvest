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

import cz.registrdigitalizace.harvest.db.Metadata.MetadataItem;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * METADATA table accessor.
 * 
 * @author Jan Pokorsky
 */
public class MetadataDao {
    
    private static final Logger LOG = Logger.getLogger(MetadataDao.class.getName());
    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    /**
     * Deletes metadata of all digital objects of a library.
     * @param libraryId library ID
     * @throws DaoException failure
     */
    public void delete(BigDecimal libraryId) throws DaoException {
        try {
            LOG.log(Level.FINE, "delete from METADATA where ID in (select ID from DIGOBJEKT where RDIGKNIHOVNA_DIGOBJEKT={0})", libraryId);
            Connection c = source.getConnection();
            PreparedStatement stmt = c.prepareStatement(
                    "delete from METADATA where ID in (select ID from DIGOBJEKT where RDIGKNIHOVNA_DIGOBJEKT=?)");
            try {
                stmt.setBigDecimal(1, libraryId);
                stmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(stmt);
            }
        } catch (SQLException ex) {
            throw new DaoException(ex);
        } finally {
            LOG.fine("delete done");
        }
    }

    /**
     * Deletes metadata of a digital object.
     * @param m metadata with digObjId
     * @throws DaoException failure
     */
    public void delete(Metadata m) throws DaoException {
        if (m.getDigObjId() == null) {
            throw new IllegalArgumentException("Missing digObjId");
        }
        try {
            Connection c = source.getConnection();
            PreparedStatement stmt = c.prepareStatement(
                    "delete from METADATA where RDIGOBJEKT_METADATA=?");
            try {
                stmt.setBigDecimal(1, m.getDigObjId());
                stmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(stmt);
            }
        } catch (SQLException ex) {
            throw new DaoException(ex);
        } finally {
            LOG.fine("delete done");
        }
    }

    /**
     * Inserts metadata of a digital object.
     * @param metadataId ID sequence
     * @param m metadata
     * @throws DaoException failure
     */
    public void insert(IdSequence metadataId, Metadata m) throws DaoException {
        try {
            Connection c = source.getConnection();
            PreparedStatement stmt = c.prepareStatement(
                    "insert into METADATA (ID,VALUE,VALID,RELIEFNAME,RDIGOBJEKT_METADATA)"
                    + " values (?,?,?,?,?)");
            try {
                for (MetadataItem item : m.getItems()) {
                    item.setId(metadataId.increment());
                    insert(item, m, stmt);
                }
                int[] results = stmt.executeBatch();
                for (int result : results) {
                    if (result == Statement.EXECUTE_FAILED) {
                        throw new DaoException("insert failed " + m);
                    }
                }
            } finally {
                SQLQuery.tryClose(stmt);
            }
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void insert(MetadataItem mi, Metadata m, PreparedStatement stmt) throws SQLException {
        int i = 1;
        stmt.setBigDecimal(i++, mi.getId());
        stmt.setString(i++, value(mi.getValue(), mi.getReliefName(), 2000, null));
        stmt.setBoolean(i++, !mi.isInvalid());
        stmt.setString(i++, mi.getReliefName());
        stmt.setBigDecimal(i++, m.getDigObjId());
        stmt.addBatch();
    }

    private String value(String value, String name, int limit, Object metadata) {
        if (value != null && value.length() > limit / 3) {
            try {
                Utf8LengthStream ls = new Utf8LengthStream();
                OutputStreamWriter w = new OutputStreamWriter(ls, "UTF-8");
                int endIndex = -1;
                for (int i = 0; i < value.length(); i++) {
                    w.write(value.charAt(i));
                    w.flush();
                    if (endIndex < 0 && ls.length > limit) {
                        endIndex = i;
                    }
                }
                if (endIndex >= 0) {
                    LOG.log(Level.WARNING, String.format("%s: length: %s, limit:%s, value: %s\n%s",
                            name, ls.length, limit, value, metadata), new IllegalStateException());
                    value = value.substring(0, endIndex);
                }
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }
        return value;
    }

    private static final class Utf8LengthStream extends OutputStream {

        int length = 0;

        @Override
        public void write(int b) throws IOException {
            ++length;
        }

    }
    
}
