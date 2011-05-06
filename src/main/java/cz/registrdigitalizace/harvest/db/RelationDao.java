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

import cz.registrdigitalizace.harvest.HarvestedRecord;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Jan Pokorsky
 */
public class RelationDao {
    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    public void insert(IdSequence relationId, HarvestedRecord rec, BigDecimal libraryId) throws DaoException {
        try {
            doInsert(relationId, rec, libraryId);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doInsert(IdSequence relationId, HarvestedRecord rec, BigDecimal libraryId) throws SQLException, DaoException {
        Connection connection = source.getConnection();
        int order = 1;
        PreparedStatement pstmt = connection.prepareStatement(
                "insert into DIGVAZBY (ID, PREDEK, POTOMEK, PORADI, DIGKNIHOVNA) values (?, ?, ?, ?, ?)");
        try {
            if (rec.isRoot()) {
                int col = 1;
                pstmt.setBigDecimal(col++, relationId.increment());
                pstmt.setBigDecimal(col++, null);
                pstmt.setString(col++, rec.getUuid());
                pstmt.setInt(col++, 1);
                pstmt.setBigDecimal(col++, libraryId);
                pstmt.addBatch();
            }
            for (String uuid : rec.getChildren()) {
                int col = 1;
                pstmt.setBigDecimal(col++, relationId.increment());
                pstmt.setBigDecimal(col++, rec.getId());
                pstmt.setString(col++, uuid);
                pstmt.setInt(col++, order);
                pstmt.setBigDecimal(col++, libraryId);
                pstmt.addBatch();
                order++;
            }
            int[] results = pstmt.executeBatch();
            for (int result : results) {
                if (result == Statement.EXECUTE_FAILED) {
                    throw new DaoException("insert failed");
                }
            }
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    public void delete(HarvestedRecord rec, BigDecimal libraryId) throws DaoException {
        try {
            doDelete(rec, libraryId);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doDelete(HarvestedRecord rec, BigDecimal libraryId) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "delete from DIGVAZBY where (PREDEK = ? or (PREDEK is null and POTOMEK = ?)) and DIGKNIHOVNA = ?");
        try {
            pstmt.setBigDecimal(1, rec.getId());
            pstmt.setString(2, rec.getUuid());
            pstmt.setBigDecimal(3, libraryId);
            pstmt.executeUpdate();
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

}
