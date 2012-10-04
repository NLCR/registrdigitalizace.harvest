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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatDtdDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.ext.oracle.OracleConnection;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.junit.Assert;
import org.junit.Assume;

/**
 *
 * @author Jan Pokorsky
 */
public class DbUnitSupport {
    private final DigitizationRegistrySource source;
    private IDatabaseConnection dbuConnection;


    enum Driver {ORACLE, POSTGRES}

    private Driver driver;

    public DbUnitSupport() {
        verifyDbConfiguration();
        String driverProp = System.getProperty(DigitizationRegistrySource.PROP_DRIVER);
        if ("oracle.jdbc.OracleDriver".equals(driverProp)) {
            driver = Driver.ORACLE;
        } else if ("org.postgresql.Driver".equals(driverProp)) {
            driver = Driver.POSTGRES;
        } else {
            throw new IllegalStateException("unknown driver: " + driverProp);
        }
        source = new DigitizationRegistrySource(System.getProperties());

    }

    public static void verifyDbConfiguration() {
        Assume.assumeNotNull(
                System.getProperty(DigitizationRegistrySource.PROP_URL),
                System.getProperty(DigitizationRegistrySource.PROP_DRIVER),
                System.getProperty(DigitizationRegistrySource.PROP_USERNAME),
                System.getProperty(DigitizationRegistrySource.PROP_PASSWORD)
                );
    }

    public IDatabaseConnection getConnection() throws DatabaseUnitException, SQLException {
        if (dbuConnection == null) {
            dbuConnection = createConnection();
        }
        return dbuConnection;
    }

    public void close() throws SQLException {
        if (dbuConnection != null) {
            dbuConnection.close();
        }
    }
    
    public IDatabaseConnection createConnection() throws DatabaseUnitException, SQLException {
        switch (driver) {
            case ORACLE:
                return createOracleConnection(source.getConnection());
            case POSTGRES:
                return createProgresConnection(source.getConnection());
        }
        throw new IllegalStateException();
    }

    public DigitizationRegistrySource getSource() {
        return source;
    }

    public IDataSet loadFlatXmlDataStream(Class c, String resource) throws Exception {
        return loadFlatXmlDataStream(c, resource, true);
    }

    public IDataSet loadFlatXmlDataStream(Class c, String resource, boolean usedtd) throws Exception {
//        FlatDtdDataSet.write(getConnection().createDataSet(), System.out);
        FlatXmlDataSetBuilder builder = new FlatXmlDataSetBuilder();
        if (usedtd) {
            builder.setMetaDataSetFromDtd(getResourceStream(c, "dataset.dtd"));
        }
//        builder.setMetaDataSet(getConnection().createDataSet());
        FlatXmlDataSet fds = builder.build(getResourceStream(c, resource));
        return fds;
    }

    private InputStream getResourceStream(Class c, String resource) {
        InputStream stream = c.getResourceAsStream(resource);
        Assert.assertNotNull("stream.name: " + resource + ", class: " + c, stream);
        return stream;
    }

    public void printTableAsFlatXml(String... tableNames) throws Exception {
        if (tableNames == null || tableNames.length == 0) {
            throw new IllegalStateException();
        }
        QueryDataSet queryDataSet = new QueryDataSet(getConnection());
        for (String table : tableNames) {
            queryDataSet.addTable(table);
        }
        FlatXmlDataSet.write(queryDataSet, System.out);
    }

    public void dumpTable(ITable table) throws DataSetException {
        System.out.println("--dump table: " + table.getTableMetaData().getTableName());
        for (int i = 0, j = table.getRowCount(); i < j; i++) {
            Column[] columns = table.getTableMetaData().getColumns();
            System.out.print("row: " + i);
            for (int k = 0; k < columns.length; k++) {
                Column column = columns[k];
                String columnName = column.getColumnName();
                System.out.printf(", %s: %s", columnName, table.getValue(i, columnName));
            }
            System.out.println();
        }
        System.out.println("--------------");
    }

    private IDatabaseConnection createOracleConnection(Connection c) throws DatabaseUnitException {
        String userProp = System.getProperty(DigitizationRegistrySource.PROP_USERNAME);
        Assert.assertNotNull(DigitizationRegistrySource.PROP_USERNAME, userProp);
        return new OracleConnection(c, userProp);
    }

    private IDatabaseConnection createProgresConnection(Connection c) throws DatabaseUnitException {
        DatabaseConnection dbc = new DatabaseConnection(c);
        DatabaseConfig config = dbc.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
        // Progress cannot handle columns names like XML thus we have to escape them.
        config.setProperty(DatabaseConfig.PROPERTY_ESCAPE_PATTERN, "\"?\"");
        return dbc;
    }


}
