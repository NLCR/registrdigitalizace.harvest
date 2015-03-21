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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Jan Pokorsky
 */
public final class LibraryDao {

    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    public List<Library> find() throws DaoException {
        try {
            return doFind();
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private List<Library> doFind() throws SQLException {
        Connection conn = source.getConnection();
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(
                    "select K.ID as ID, L.\"VALUE\" as DLISTS_VALUE, PROTOKOL, FORMATDAT, LASTHARVEST, OAIPMHCOMMAND, OAIPMHSERVERBASEURL"
                    + " from DIGKNIHOVNA K, DLISTS L where K.ID = L.ID");
            try {
                return processSelectLibraries(rs);
            } finally {
                SQLQuery.tryClose(rs);
            }
        } finally {
            SQLQuery.tryClose(stmt);
        }
    }

    private List<Library> processSelectLibraries(ResultSet rs) throws SQLException {
        List<Library> libraries = new ArrayList<Library>();
        while (rs.next()) {
            Library library = new Library();
            library.setId(rs.getBigDecimal("ID"));
            library.setHarvestProtocol(rs.getString("PROTOKOL"));
            library.setMetadataFormat(rs.getString("FORMATDAT"));
            library.setLastHarvest(rs.getString("LASTHARVEST"));
            library.setQueryParameters(rs.getString("OAIPMHCOMMAND"));
            library.setBaseUrl(rs.getString("OAIPMHSERVERBASEURL"));
            library.setDListValue(rs.getString("DLISTS_VALUE"));
            libraries.add(library);
        }
        return libraries;
    }

    public void update(Library library) throws DaoException {
        try {
            doUpdate(library);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    private void doUpdate(Library library) throws SQLException {
        Connection conn = source.getConnection();
        PreparedStatement pstmt = conn.prepareStatement(
                "update DIGKNIHOVNA set LASTHARVEST=? where ID=?");
        try {
            pstmt.setString(1, library.getLastHarvest());
            pstmt.setBigDecimal(2, library.getId());
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }

}
