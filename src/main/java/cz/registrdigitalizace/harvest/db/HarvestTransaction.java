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
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jan Pokorsky
 */
public class HarvestTransaction {

    private final DigitizationRegistrySource source;
    private Connection connection;

    public HarvestTransaction(DigitizationRegistrySource source) {
        this.source = source;
    }

    public void begin() throws DaoException {
        if (connection != null) {
            throw new DaoException("Cannot begin running transaction.");
        }
        try {
            connection = source.getConnection();
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    public void commit() throws DaoException {
        if (connection == null) {
            throw new DaoException("Not running transaction.");
        }
        try {
            connection.commit();
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    public void close() throws DaoException {
        if (connection == null) {
            return ;
        }
        try {
            connection.close();
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
        connection = null;
    }

    public void rollback() {
        if (connection == null) {
            return ;
        }
        try {
            connection.rollback();
        } catch (SQLException ex) {
            // cannot do more than log it
            Logger.getLogger(HarvestTransaction.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Connection getConnection() {
        return connection;
    }

}
