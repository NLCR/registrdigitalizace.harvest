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

import cz.registrdigitalizace.harvest.Utils;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DIGMETADATA table accessor.
 * 
 * @author Jan Pokorsky
 */
public class MetadataDao {
    
    private static final Logger LOG = Logger.getLogger(MetadataDao.class.getName());
    private HarvestTransaction source;

    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }
    
    public void delete() throws DaoException {
        try {
            LOG.fine("delete from DIGMETADATA");
            Connection c = source.getConnection();
            PreparedStatement stmt = c.prepareStatement("delete from DIGMETADATA");
            try {
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
    
    public void insert(Metadata m) throws DaoException {
        try {
            Connection c = source.getConnection();
            PreparedStatement stmt = c.prepareStatement(
                    "insert into DIGMETADATA (ID,NAZEV,ISSN,ISBN,CCNB,SIGLA,"
                    + "SIGNATURA,AUTORI,VYDAVATELE,ROKVYD,MODIFIKACE)"
                    + " values (?,?,?,?,?,?,?,?,?,?,?)");
            try {
                int i = 1;
                stmt.setBigDecimal(i++, m.getId());
                stmt.setString(i++, value(m.getTitle(), "title", 2000, m));
                stmt.setString(i++, value(m.getIssn(), "issn", 255, m));
                stmt.setString(i++, value(m.getIsbn(), "isbn", 255, m));
                stmt.setString(i++, value(m.getCcnb(), "ccnb", 255, m));
                stmt.setString(i++, value(m.getSigla(), "sigla", 255, m));
                stmt.setString(i++, value(m.getSignature(), "signature", 2000, m));
                stmt.setString(i++, value(m.getAuthors(), "authors", 2000, m));
                stmt.setString(i++, value(m.getPublishers(), "publishers", 2000, m));
                stmt.setString(i++, value(m.getYearOfPublication(), "yearOfPublication", 255, m));
                stmt.setInt(i++, 1);
                stmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(stmt);
            }
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }
    
    public void update(Metadata m) throws DaoException {
        try {
            Connection c = source.getConnection();
            PreparedStatement stmt = c.prepareStatement(
                    "update DIGMETADATA set NAZEV=?,ISSN=?,ISBN=?,CCNB=?,SIGLA=?,"
                    + "SIGNATURA=?,AUTORI=?,VYDAVATELE=?,ROKVYD=?,MODIFIKACE=?"
                    + " where ID=?");
            try {
                int i = 1;
                stmt.setString(i++, value(m.getTitle(), "title", 2000, m));
                stmt.setString(i++, value(m.getIssn(), "issn", 255, m));
                stmt.setString(i++, value(m.getIsbn(), "isbn", 255, m));
                stmt.setString(i++, value(m.getCcnb(), "ccnb", 255, m));
                stmt.setString(i++, value(m.getSigla(), "sigla", 255, m));
                stmt.setString(i++, value(m.getSignature(), "signature", 2000, m));
                stmt.setString(i++, value(m.getAuthors(), "authors", 2000, m));
                stmt.setString(i++, value(m.getPublishers(), "publishers", 2000, m));
                stmt.setString(i++, value(m.getYearOfPublication(), "yearOfPublication", 255, m));
                stmt.setInt(i++, 1);
                stmt.setBigDecimal(i++, m.getId());
                stmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(stmt);
            }
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }
    
    public long updateDigObjectMetadata() throws DaoException {
        try {
            Connection c = source.getConnection();
            computeModifications(c);
            // commit changes in temporary tables to minimize transaction log
            c.commit();

            PreparedStatement stmt = c.prepareStatement(SQLQuery.getSelectMetadataHierarchy());
            PreparedStatement updateStmt = c.prepareStatement(
                    "update DIGOBJEKT set NAZEV=?, AUTOR=?, ISSN=?, ISBN=?, CCNB=?,"
                    + " SIGLA=?, SIGNATURA=?, VYDAVATEL=?, ROKVYD=? where ID=?");
            PreparedStatement resetChangesStmt = c.prepareStatement(
                    "update DIGMETADATA set MODIFIKACE=0 where MODIFIKACE > 0");
            
            LOG.log(Level.FINE, "update DIGOBJEKT metadata as \n{0}",
                    SQLQuery.getSelectMetadataHierarchy());
            long start = System.currentTimeMillis();
            long updateNumber = 1;
            try {
                ResultSet rs = stmt.executeQuery();
                LOG.log(Level.FINE, "hierarchical select executed");
                boolean finest = LOG.isLoggable(Level.FINEST);
                try {
                    while (rs.next()) {
                        String titles = rs.getString("NAZEV");
                        String authors = rs.getString("AUTOR");
                        String issn = rs.getString("ISSN");
                        String isbn = rs.getString("ISBN");
                        String ccnb = rs.getString("CCNB");
                        String siglas = rs.getString("SIGLA");
                        String signatures = rs.getString("SIGNATURA");
                        String publishers = rs.getString("VYDAVATEL");
                        String years = rs.getString("ROKVYD");
                        BigDecimal id = rs.getBigDecimal("ID");
                        if (finest) {                     
                            String up = String.format(
                                "update DIGOBJEKT set NAZEV='%s', AUTOR='%s', ISSN='%s',"
                                    + " ISBN='%s', CCNB='%s', SIGLA='%s', SIGNATURA='%s',"
                                    + " VYDAVATEL='%s', ROKVYD='%s' where ID=%s",
                                    titles, authors, issn, isbn, ccnb, siglas,
                                    signatures, publishers, years, id
                                    );
                            LOG.log(Level.FINEST, "udpdating... {0}\n{1}",
                                    new Object[] {updateNumber, up});
                        }
                        int col = 1;
                        updateStmt.setString(col++, titles);
                        updateStmt.setString(col++, authors);
                        updateStmt.setString(col++, issn);
                        updateStmt.setString(col++, isbn);
                        updateStmt.setString(col++, ccnb);
                        updateStmt.setString(col++, siglas);
                        updateStmt.setString(col++, signatures);
                        updateStmt.setString(col++, publishers);
                        updateStmt.setString(col++, years);
                        updateStmt.setBigDecimal(col++, id);
                        SQLQuery.assertRows(1, updateStmt.executeUpdate());
                        updateNumber++;
                    }
                } finally {
                    SQLQuery.tryClose(rs);
                }
            } finally {
                SQLQuery.tryClose(stmt);
                SQLQuery.tryClose(updateStmt);
            }
            long end = System.currentTimeMillis() - start;
            LOG.log(Level.FINE, "update of {1} object(s) finished after: {0} ",
                    new Object[] {Utils.elapsedTime(end), updateNumber});
            
            try {
                resetChangesStmt.executeUpdate();
            } finally {
                SQLQuery.tryClose(resetChangesStmt);
            }
            
            return updateNumber - 1;
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }
    
    private void computeModifications(Connection c) throws SQLException {
        Statement stmt = c.createStatement();
        try {
            long start = System.currentTimeMillis();
            LOG.fine("delete from DIGMETADATA_CHANGES");
            stmt.executeUpdate("delete from DIGMETADATA_CHANGES");
            long end = System.currentTimeMillis() - start;
            LOG.log(Level.FINE, "delete finished after: {0}", Utils.elapsedTime(end));

            start = System.currentTimeMillis();
            LOG.fine("insert DIGMETADATA_CHANGES");
            stmt.executeUpdate(SQLQuery.getInsertMetadataChanges());
            end = System.currentTimeMillis() - start;
            LOG.log(Level.FINE, "insert finished after: {0}", Utils.elapsedTime(end));
        } finally {
            SQLQuery.tryClose(stmt);
        }
    }

    private String value(String value, String name, int limit, Metadata m) {
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
                            name, ls.length, limit, value, m), new IllegalStateException());
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
