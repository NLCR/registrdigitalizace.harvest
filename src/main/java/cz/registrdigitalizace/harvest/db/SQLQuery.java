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

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jan Pokorsky
 */
final class SQLQuery {
    private static final Logger LOGGER = Logger.getLogger(SQLQuery.class.getName());

    private static String insertLokace;
    private static String insertVazby;
    private static String deleteUnrelatedDigiObjekts;
    private static String findMissingThumbnails;
    private static String selectMetadataHierarchy;
    private static String insertMetadataChanges;

    public static void init() throws IOException {
        CharArrayWriter queryBuilder = new CharArrayWriter(1024);
        insertLokace = loadQuery("/sql/insertLokace.sql", queryBuilder);
        insertVazby = loadQuery("/sql/insertVazby.sql", queryBuilder);
        deleteUnrelatedDigiObjekts = loadQuery("/sql/deleteUnrelatedDigiObjekts.sql", queryBuilder);
        findMissingThumbnails = loadQuery("/sql/findMissingThumbnails.sql", queryBuilder);
        selectMetadataHierarchy = loadQuery("/sql/selectMetadataHierarchy.sql", queryBuilder);
        insertMetadataChanges = loadQuery("/sql/insertMetadataChanges.sql", queryBuilder);
    }

    public static String getInsertLokace() {
        return insertLokace;
    }

    public static String getInsertVazby() {
        return insertVazby;
    }

    public static String getDeleteUnrelatedDigiObjects() {
        return deleteUnrelatedDigiObjekts;
    }

    public static String getFindMissingThumbnails() {
        return findMissingThumbnails;
    }

    public static String getSelectMetadataHierarchy() {
        return selectMetadataHierarchy;
    }

    public static String getInsertMetadataChanges() {
        return insertMetadataChanges;
    }

    private static String loadQuery(String filename, CharArrayWriter queryBuilder) throws IOException {
        InputStream input = SQLQuery.class.getResourceAsStream(filename);
        Reader reader = new InputStreamReader(input, "UTF-8");
        queryBuilder.reset();
        try {
            for (int read = reader.read(); read >= 0 ; read = reader.read()) {
                queryBuilder.write(read);
            }
            queryBuilder.flush();
            return queryBuilder.toString();
        } finally {
            reader.close();
        }
    }

    public static void assertRows(int expectedRows, int resultRows) throws SQLException {
        if (expectedRows != resultRows) {
            throw new SQLException(String.format(
                    "expected: %s, result: %s", expectedRows, resultRows));
        }
    }

    public static void tryClose(Connection c) {
        try {
            c.close();
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    public static void tryClose(Statement stmt) {
        try {
            stmt.close();
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    public static void tryClose(ResultSet rs) {
        try {
            rs.close();
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

}
