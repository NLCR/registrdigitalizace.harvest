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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jan Pokorsky
 */
public final class DigitizationRegistrySource {

    private static final Logger LOGGER = Logger.getLogger(DigitizationRegistrySource.class.getName());

    static final String PROP_URL = "drharvest.jdbc.url";
    static final String PROP_DRIVER = "drharvest.jdbc.driver";
    static final String PROP_USERNAME = "drharvest.jdbc.user";
    static final String PROP_PASSWORD = "drharvest.jdbc.password";
    
    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DigitizationRegistrySource(Properties config) {
        if (config == null) {
             throw new NullPointerException("config");
        }

        this.jdbcUrl = config.getProperty(PROP_URL);
        if (this.jdbcUrl == null) {
             throw new NullPointerException(PROP_URL);
        }
        this.username = config.getProperty(PROP_USERNAME);
        this.password = config.getProperty(PROP_PASSWORD);
        String driver = config.getProperty(PROP_DRIVER);
        LOGGER.log(Level.INFO, "url: {0}, driver: {1}, user: {2}", new Object[]{jdbcUrl, driver, username});
        initDriver(driver);
        try {
            SQLQuery.init();
        } catch (IOException ex) {
            // this should not occur
            throw new IllegalStateException(ex);
        }
    }

    public Connection getConnection() throws SQLException {
        SQLException sex = null;
        // workaround to connect to faraway Oracle in several attempts
        for (int i = 1; i <= 10; i++) {
            try {
                Connection conn = getConnectionImpl();
                LOGGER.log(Level.FINE, "{0}. getConnection successful.", i);
                return conn;
            } catch (SQLException ex) {
                LOGGER.log(Level.WARNING, "{0}. getConnection failed.", i);
                sex = ex;
            }
        }
        throw sex;
    }
    
    public Connection getConnectionImpl() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        // allow to change schema?
        // ALTER SESSION SET CURRENT_SCHEMA=new_schema
        return connection;
    }

    private static void initDriver(String className) {
        if (className == null) {
            return ;
        }
        try {
            Class.forName(className);
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException("class: '" + className + "'", ex);
        }
    }
}
