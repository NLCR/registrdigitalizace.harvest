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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 *
 * @author Jan Pokorsky
 */
public class LocationDao {

    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    /**
     * Finds location ID for given digital object and library
     * @return LOKACE.ID
     */
    public BigDecimal find(BigDecimal digiObjId, BigDecimal libraryId) throws DaoException {
        try {
            return doFind(digiObjId, libraryId);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }
    private BigDecimal doFind(BigDecimal digiObjId, BigDecimal libraryId) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "select L.ID ID from LOKACE L, DLISTS DL where RDIGOBJEKTL = ? and DIGKNIHOVNA = DL.\"VALUE\" and DL.ID = ?");
        try {
            pstmt.setBigDecimal(1, digiObjId);
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

    public void insert(BigDecimal locationId, BigDecimal digiObjectId,
            BigDecimal libraryId, Date inputDate) throws DaoException {
        try {
            doInsert(locationId, digiObjectId, libraryId, inputDate);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doInsert(BigDecimal locationId, BigDecimal digiObjectId,
            BigDecimal libraryId, Date inputDate) throws SQLException {
        Connection connection = source.getConnection();
        // insert (ID, DATUMZAL, RDIGOBJEKTL)
        PreparedStatement pstmt = connection.prepareStatement(SQLQuery.getInsertLokace());
        try {
            pstmt.setBigDecimal(1, locationId);
            pstmt.setDate(2, inputDate);
            pstmt.setBigDecimal(3, digiObjectId);
            pstmt.setBigDecimal(4, libraryId);
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    public void update(BigDecimal locationId, Date inputDate) throws DaoException {
        try {
            doUpdate(locationId, inputDate);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doUpdate(BigDecimal locationId, Date inputDate) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "update LOKACE set DATUMZAL=? where ID=?");
        try {
            pstmt.setDate(1, inputDate);
            pstmt.setBigDecimal(2, locationId);
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    /**
     * Deletes collection of locations.
     * @param locationIds collection of IDs to delete
     * @param libraryId library filter
     */
    public void delete(Collection<BigDecimal> locationIds) throws DaoException {
        try {
            doDelete(locationIds);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doDelete(Collection<BigDecimal> locationIds) throws SQLException {
        if (locationIds.isEmpty()) {
            return ;
        }
        Connection connection = source.getConnection();
        StringBuilder sb = new StringBuilder("delete from LOKACE where ID in (");
        for (BigDecimal locationId : locationIds) {
            sb.append(locationId).append(',');
        }
        sb.setCharAt(sb.length() - 1, ')');
        Statement stmt = connection.createStatement();
        try {
            int expectedRows = locationIds.size();
            SQLQuery.assertRows(expectedRows, stmt.executeUpdate(sb.toString()));
        } finally {
            SQLQuery.tryClose(stmt);
        }
    }

}
