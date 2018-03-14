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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jan Pokorsky
 */
public final class LibraryDao {
    private static final Logger LOG = Logger.getLogger(LibraryDao.class.getName());

    private HarvestTransaction source;
    public String idVybranychKnihoven;
    public String fromDate;
    public String toDate;

    /**
     * nastavuje dataSource
     * @param source (HarvestTransaction)
     */
    public void setDataSource(HarvestTransaction source) {
        this.source = source;
    }

    /**
     * volá metodu doFind
     * @return (List<Library>)
     * @throws DaoException 
     */
    public List<Library> find() throws DaoException {
        try {
            LOG.log(Level.INFO, " vybrané knihovny: " + this.idVybranychKnihoven);
            return doFind();
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    /**
     * načte záznamy vybraných knihoven a ty zpracuje
     * @return (List<Library>)
     * @throws SQLException 
     */
    private List<Library> doFind() throws SQLException {
        LOG.log(Level.INFO, " načítám knihovny z databáze");
        String vybraneKnihovny = "";
        Connection conn = source.getConnection();
        Statement stmt = conn.createStatement();
        try {
            if (!Utils.jePrazdne(this.idVybranychKnihoven)) {
                vybraneKnihovny = " and K.ID in (" + this.idVybranychKnihoven + ")";
            }
            String pomocneSQL = "select K.ID as ID, L.\"VALUE\" as DLISTS_VALUE, PROTOKOL, FORMATDAT, LASTHARVEST, OAIPMHCOMMAND, OAIPMHSERVERBASEURL, KONTAKT"
                    + " from DIGKNIHOVNA K, DLISTS L where K.ID = L.ID and OAIPMHSERVERBASEURL is not null" + vybraneKnihovny;
            LOG.log(Level.INFO, "sql: " + pomocneSQL);
            ResultSet rs = stmt.executeQuery(pomocneSQL);
            try {
                return processSelectLibraries(rs);
            } finally {
                SQLQuery.tryClose(rs);
            }
        } finally {
            SQLQuery.tryClose(stmt);
        }
    }

    /**
     * nastaví hodnoty pro jednotlivé knihovny, tak jak jsou předány v resultSetu
     * @param rs (ResultSet)
     * @return (List<Library>)
     * @throws SQLException 
     */
    private List<Library> processSelectLibraries(ResultSet rs) throws SQLException {
        LOG.log(Level.INFO, " generuji záznamy knihoven");
        List<Library> libraries = new ArrayList<Library>();
        while (rs.next()) {
            LOG.log(Level.INFO, " knihovna: " + rs.getBigDecimal("ID"));
            Library library = new Library();
            library.setId(rs.getBigDecimal("ID"));
            library.setHarvestProtocol(rs.getString("PROTOKOL"));
            library.setMetadataFormat(rs.getString("FORMATDAT"));
            library.setLastHarvest(rs.getString("LASTHARVEST"));
            library.setQueryParameters(rs.getString("OAIPMHCOMMAND"));
            library.setBaseUrl(rs.getString("OAIPMHSERVERBASEURL"));
            library.setDListValue(rs.getString("DLISTS_VALUE"));
            library.setKontakt(rs.getString("KONTAKT"));
//nastaveni posledniho uspesneho datumu harvesteru nebo pozadovane hodnoty - pozor na format datumu
            if ((fromDate == null) || ("".equals(fromDate))) {
                library.setFromDate(rs.getString("LASTHARVEST"));
            } else {
                library.setFromDate(fromDate);
            }
            if ((toDate == null) || ("".equals(toDate))) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date datumDnesek = new Date();
                String datumPomocny = simpleDateFormat.format(datumDnesek) + "T00:00:00Z";
                library.setToDate(datumPomocny);
            } else {
                library.setToDate(toDate);
            }
            libraries.add(library);
        }
        return libraries;
    }

    /**
     * spouští metodu pro úpravu datumu posledního zpracovaného záznamu
     * @param library (Library)
     * @throws DaoException 
     */
    public void update(Library library) throws DaoException {
        try {
            doUpdate(library);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    /**
     * upraví v databázi u vybrané knihovny hodnotu na datum (a čas) posledního zpracovaného záznamu
     * @param library (Library)
     * @throws SQLException 
     */
    private void doUpdate(Library library) throws SQLException {
        Connection conn = source.getConnection();
        PreparedStatement pstmt = conn.prepareStatement("update DIGKNIHOVNA set LASTHARVEST=? where ID=?");
        try {
//            pstmt.setString(1, library.getLastHarvest()); //
            pstmt.setString(1, library.getToDate());
            pstmt.setBigDecimal(2, library.getId());
            SQLQuery.assertRows(1, pstmt.executeUpdate());
        } finally {
            SQLQuery.tryClose(pstmt);
        }
    }
    
    /**
     * nastavuje id vybraných knihoven
     * @param idVybranychKnihovnen (String)
     */
    public void setVybraneKnihovny(String value) {
        if (!Utils.jePrazdne(value)) {
            this.idVybranychKnihoven = "" + value;
            LOG.log(Level.INFO, " knihovny nastaveny " + this.getVybraneKnihovny());
        }
    }

    /**
     * vrací vybrané knihovny
     * @return (String)
     */
    public String getVybraneKnihovny() {
        return idVybranychKnihoven;
    }

    /**
     * nastaví datum (a čas) od kdy se má harvestrovat
     * formát je: yyyy-MM-dd'T'HH:mm:ss'Z'
     * @param date (String)
     */
    public void setFromDate(String date) {
        if ((date != null) && (!"".equals(date))) {
            this.toDate = "" + date;
        }
    }

    /**
     * vrací datum (a čas) od kdy se má harvestrovat
     * formát je: yyyy-MM-dd'T'HH:mm:ss'Z'
     * @return (String)
     */
    public String getFromDate() {
        return this.fromDate;
    }

    /**
     * nastaví datum (a čas) do kdy se má harvestrovat
     *  vždy se nastaví půlnoc tohoto dne, takže se zpracová pouze předchozí den
     * formát je: yyyy-MM-dd'T'HH:mm:ss'Z'
     * @param date (String)
     */
    public void setToDate(String date) {
        if (!Utils.jePrazdne(date)) {
            this.toDate = "" + date;
        }
    }

    /**
     * vrací datum (a čas) do kdy se má harvestrovat
     *  vždy se nastaví půlnoc tohoto dne, takže se zpracová pouze předchozí den
     * formát je: yyyy-MM-dd'T'HH:mm:ss'Z'
     * @return (String)
     */
    public String getToDate() {
        return this.toDate;
    }

}
