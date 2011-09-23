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
     * @return DIGIOBJEKT.ID
     */
    public BigDecimal find(String uuid) throws DaoException {
        try {
            return doFind(uuid);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private BigDecimal doFind(String uuid) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "select ID from DIGOBJEKT where UUID = ?");
        try {
            pstmt.setString(1, uuid);
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
     * Finds MODS metadata of every digital object.
     * @param uuid UUID to search
     * @return collection of 
     */
    public IterableResult<DigObject> findMods() throws DaoException {
        try {
            return doFindMods();
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private IterableResult<DigObject> doFindMods() throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "select ID, \"XML\" from DIGOBJEKT");
        ResultSet rs = null;
        try {
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

    public void insert(BigDecimal id, String uuid, String type, String xml) throws DaoException {
        try {
            doInsert(id, uuid, type, xml);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doInsert(BigDecimal id, String uuid, String type, String xml) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "insert into DIGOBJEKT"
                + " (ID, UUID, DRUHDOKUMENTU, \"XML\")"
                + " values (?, ?, ?, ?)");
        try {
            int col = 1;
            pstmt.setBigDecimal(col++, id);
            pstmt.setString(col++, uuid);
            pstmt.setString(col++, type);
            pstmt.setString(col++, xml);
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    public void update(BigDecimal id, String type, String xml) throws DaoException {
        try {
            doUpdate(id, type, xml);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doUpdate(BigDecimal id, String type, String xml) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "update DIGOBJEKT"
                + " set DRUHDOKUMENTU = ?, \"XML\" = ?"
                + " where ID = ?");
        try {
            int col = 1;
            pstmt.setString(col++, type);
            pstmt.setString(col++, xml);
            pstmt.setBigDecimal(col++, id);
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    /**
     * Removes all digitized objects without any existing relation.
     */
    public void removeUnrelated() throws DaoException {
        try {
            Connection connection = source.getConnection();
            PreparedStatement pstmt = connection.prepareStatement(
                    "delete from XPREDDIGOBJ where RDIGOBJEKT in (select ID from DIGOBJEKT where UUID not in (select POTOMEK from DIGVAZBY))");
            try {
                pstmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(pstmt);
            }

            pstmt = connection.prepareStatement(
                    "delete from DIGMETADATA where ID in (select ID from DIGOBJEKT where UUID not in (select POTOMEK from DIGVAZBY))");
            try {
                pstmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(pstmt);
            }

            pstmt = connection.prepareStatement(
                    "delete from DIGMETADATA_CHANGES");
            try {
                pstmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(pstmt);
            }

            pstmt = connection.prepareStatement(
                    "delete from DIGOBJEKT where UUID not in (select POTOMEK from DIGVAZBY)");
            try {
                pstmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(pstmt);
            }
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

}
