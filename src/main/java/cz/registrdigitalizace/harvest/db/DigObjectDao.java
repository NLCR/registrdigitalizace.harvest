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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 *
 * @author Jan Pokorsky
 */
public class DigObjectDao {

    private HarvestTransaction source;

    public DigObjectDao() {
    }

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    /**
     * Finds ID for particular UUID.
     * @param uuid UUID to search
     * @return DIGOBJEKT.ID
     */
    public BigDecimal find(BigDecimal libraryId, String uuid) throws DaoException {
        try {
            return doFind(libraryId, uuid);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private BigDecimal doFind(BigDecimal libraryId, String uuid) throws SQLException {
        if (uuid == null || libraryId == null) {
            throw new IllegalArgumentException(String.format("uuid: %s, lib: %s", uuid, libraryId));
        }
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "select ID from DIGOBJEKT where UUID = ? and RDIGKNIHOVNA_DIGOBJEKT = ?");
        try {
            pstmt.setString(1, uuid);
            pstmt.setBigDecimal(2, libraryId);
            ResultSet rs = pstmt.executeQuery();
            try {
                if (rs.next()) {
                    BigDecimal id = rs.getBigDecimal("ID");
                    return id;
                }
                return null;
            } finally {
                SQLQuery.tryClose(rs);
            }
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    /**
     * Finds MODS metadata of every digital object of a library.
     */
    public IterableResult<DigObject> findMods(Library library) throws DaoException {
        try {
            return doFindMods(library);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private IterableResult<DigObject> doFindMods(Library library) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "select ID, \"XML\" from DIGOBJEKT where RDIGKNIHOVNA_DIGOBJEKT = ?");
        ResultSet rs = null;
        try {
            pstmt.setBigDecimal(1, library.getId());
            rs = pstmt.executeQuery();
            return new FindModsResult(pstmt, rs);
        } finally {
            if (rs == null) {
                SQLQuery.tryClose(pstmt);
            }
        }
    }
    
    private static final class FindModsResult extends IterableResult<DigObject> {

        public FindModsResult(Statement stmt, ResultSet rs) {
            super(stmt, rs);
        }

        @Override
        protected DigObject fetchNext() throws DaoException {
            try {
                BigDecimal id = rs.getBigDecimal("ID");
                String xml = rs.getString("XML");
                DigObject digObject = new DigObject();
                digObject.setId(id);
                digObject.setXml(xml);
                return digObject;
            } catch (SQLException ex) {
                throw new DaoException(ex);
            }
        }
    
    }

    public void insert(BigDecimal libraryId, BigDecimal id, String uuid,
            String type, String xml, String xmlNs, String objState, boolean isLeaf) throws DaoException {
        try {
            doInsert(libraryId, id, uuid, type, xml, xmlNs, objState, isLeaf);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doInsert(BigDecimal libraryId, BigDecimal id, String uuid,
            String type, String xml, String xmlNs, String objState, boolean isLeaf) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "insert into DIGOBJEKT"
                + " (ID, UUID, DRUHDOKUMENTU, \"XML\", FORMATXML, STAV, ZALDATE, EDIDATE, RDIGKNIHOVNA_DIGOBJEKT, ISLEAF)"
                + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        try {
            int col = 1;
            pstmt.setBigDecimal(col++, id);
            pstmt.setString(col++, uuid);
            pstmt.setString(col++, type);
            pstmt.setString(col++, xml);
            pstmt.setString(col++, xmlNs);
            pstmt.setString(col++, objState);
            pstmt.setTimestamp(col++, now); // ZALDATE
            pstmt.setTimestamp(col++, now); // EDIDATE == ZALDATE
            pstmt.setBigDecimal(col++, libraryId);
            pstmt.setInt(col++, isLeaf ? 1 : 0);
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    public void update(BigDecimal id, String type, String xml, String xmlNs, boolean isLeaf) throws DaoException {
        try {
            doUpdate(id, type, xml, xmlNs, isLeaf);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doUpdate(BigDecimal id, String type, String xml, String xmlNs, boolean isLeaf) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "update DIGOBJEKT"
                + " set DRUHDOKUMENTU = ?, \"XML\" = ?,"
                + " FORMATXML = ?,"
                + " EDIDATE = ?,"
                + " ISLEAF = ?"
                + " where ID = ?");
        try {
            int col = 1;
            pstmt.setString(col++, type);
            pstmt.setString(col++, xml);
            pstmt.setString(col++, xmlNs);
            pstmt.setTimestamp(col++, new Timestamp(System.currentTimeMillis()));
            pstmt.setInt(col++, isLeaf ? 1 : 0);
            // where
            pstmt.setBigDecimal(col++, id);
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    public boolean updateState(BigDecimal id, String state) throws DaoException {
        try {
            return doUpdateState(id, state);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private boolean doUpdateState(BigDecimal id, String state) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "update DIGOBJEKT set STAV = ?, EDIDATE = ? where ID = ?");
        try {
            int col = 1;
            pstmt.setString(col++, state);
            pstmt.setTimestamp(col++, new Timestamp(System.currentTimeMillis()));
            // where
            pstmt.setBigDecimal(col++, id);
            return pstmt.executeUpdate() == 1;
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    /**
     * Removes all digitized objects without any existing relation.
     */
    public void removeUnrelated(BigDecimal libraryId) throws DaoException {
        try {
            Connection connection = source.getConnection();
            // remove false roots that are actually children
            PreparedStatement pstmt = connection.prepareStatement(
                    "delete from DIGVAZBY where ID in (select ID from DIGVAZBY where PREDEK is null and DIGKNIHOVNA=? and POTOMEK in (select POTOMEK from DIGVAZBY where PREDEK is not null and DIGKNIHOVNA=?))");
            try {
                pstmt.setBigDecimal(1, libraryId);
                pstmt.setBigDecimal(2, libraryId);
                pstmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(pstmt);
            }

            pstmt = connection.prepareStatement(
                    "delete from XPREDDIGOBJ where RDIGOBJEKT in (select ID from DIGOBJEKT where RDIGKNIHOVNA_DIGOBJEKT=? and UUID not in (select POTOMEK from DIGVAZBY where DIGKNIHOVNA=?))");
            try {
                pstmt.setBigDecimal(1, libraryId);
                pstmt.setBigDecimal(2, libraryId);
                pstmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(pstmt);
            }

//            pstmt = connection.prepareStatement(
//                    "delete from DIGOBJEKT where RDIGKNIHOVNA_DIGOBJEKT=? and UUID not in (select POTOMEK from DIGVAZBY where DIGKNIHOVNA=?)");
//            try {
//                pstmt.setBigDecimal(1, libraryId);
//                pstmt.setBigDecimal(2, libraryId);
//                pstmt.executeUpdate();
//            } finally {
//                SQLQuery.tryClose(pstmt);
//            }

        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

}
