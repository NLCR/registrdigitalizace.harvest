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
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jan Pokorsky
 */
public class IdSequenceDao {

    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    public IdSequence find(String type) throws DaoException {
        Map<String, IdSequence> ids = find(new String[] {type});
        return ids.get(type);
    }
    
    public Map<String, IdSequence> find(String... types) throws DaoException {
        try {
            return doFind(types);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private Map<String, IdSequence> doFind(String... types) throws SQLException, DaoException {
        if (types == null || types.length == 0) {
            throw new DaoException("Missing types");
        }
        Connection connection = source.getConnection();
        StringBuilder query = new StringBuilder(
                "select ID, DESKNAME from PLAANT_IDS where DESKNAME in (");
        for (String type : types) {
            query.append('\'').append(type).append("',");
        }
        query.setCharAt(query.length() - 1, ')');
        query.append(" for update");
        Statement stmt = connection.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(query.toString());
            try {
                Map<String, IdSequence> ids = new IdentityHashMap<String, IdSequence>();
                while (rs.next()) {
                    String name = rs.getString("DESKNAME").intern();
                    BigDecimal id = rs.getBigDecimal("ID");
                    ids.put(name, new IdSequence(id, name));
                }
                return ids;
            } finally {
                SQLQuery.tryClose(rs);
            }
        } finally {
            SQLQuery.tryClose(stmt);
        }
    }

    public void update(IdSequence sequence) throws DaoException {
        try {
            doUpdate(sequence);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doUpdate(IdSequence sequence) throws SQLException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "update PLAANT_IDS set ID = ? where DESKNAME = ?");
        try {
            pstmt.setBigDecimal(1, sequence.getId());
            pstmt.setString(2, sequence.getName());
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

    public void insert(IdSequence... sequences) throws DaoException {
        try {
            doInsert(sequences);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doInsert(IdSequence... sequences) throws SQLException, DaoException {
        Connection connection = source.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(
                "insert into PLAANT_IDS (ID, DESKNAME) values (?, ?)");
        try {
            for (IdSequence sequence : sequences) {
                pstmt.setBigDecimal(1, sequence.getId());
                pstmt.setString(2, sequence.getName());
                pstmt.addBatch();
            }
            int[] results = pstmt.executeBatch();
            for (int result : results) {
                if (result == Statement.EXECUTE_FAILED) {
                    throw new DaoException("insert failed");
                }
            }
        } catch (BatchUpdateException ex) {
            SQLException nextException = ex;
            while (null != (nextException = nextException.getNextException())) {
                Logger.getLogger(IdSequenceDao.class.getName()).log(Level.SEVERE, null, nextException);
            }

            throw new DaoException(ex);
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

}
