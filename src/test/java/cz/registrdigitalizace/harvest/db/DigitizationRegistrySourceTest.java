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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Jan Pokorsky
 */
//@Ignore
public class DigitizationRegistrySourceTest {

    public DigitizationRegistrySourceTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetConnectionOnline() throws Exception {
        System.out.println("getConnection");
//        DriverManager.setLogStream(System.out);
        DigitizationRegistrySource instance = createSource();
        Connection result = instance.getConnection();
        assertNotNull(result);
        result.close();
    }

    static DigitizationRegistrySource createSource() {
        System.out.printf("## System.getProperty(\"%s\"): %s\n", DigitizationRegistrySource.PROP_URL, System.getProperty(DigitizationRegistrySource.PROP_URL));
        DigitizationRegistrySource src = new DigitizationRegistrySource(System.getProperties());
        return src;
    }

}