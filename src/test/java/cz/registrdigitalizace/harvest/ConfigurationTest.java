/*
 * Copyright (C) 2012 Jan Pokorsky
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cz.registrdigitalizace.harvest;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Jan Pokorsky
 */
public class ConfigurationTest {

    public ConfigurationTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testHelp() {
        String[] args = {"-help"};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(true, false, false, false, null, false, false, null, result);
    }

    @Test
    public void testVerison() {
        String[] args = {"-version"};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(false, true, false, false, null, false, false, null, result);
    }

    @Test
    public void testUpdateMetadata() {
        String[] args = {"-updateMetadata"};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(false, false, true, false, null, false, false, null, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateMetadataAndHarvestFromCache() {
        String[] args = {"-updateMetadata", "-harvestFromCache"};
        Configuration result = Configuration.fromCmdLine(args);
    }

    @Test
    public void testHarvestFromCache() {
        String path = "/tmp";
        String[] args = {"-harvestFromCache", path};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(false, false, false, true, path, false, false, null, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHarvestFromCacheWithoutPath() {
        String[] args = {"-harvestFromCache"};
        Configuration result = Configuration.fromCmdLine(args);
    }

    @Test
    public void testHarvestToCache() {
        String[] args = {"-harvestToCache"};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(false, false, false, false, null, true, false, null, result);
    }

    @Test
    public void testHarvestToCacheWithRoot() {
        String path = "/tmp";
        String[] args = {"-harvestToCache", "-cacheRoot", path};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(false, false, false, false, null, true, false, path, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHarvestToCacheMissingRoot() {
        String[] args = {"-harvestToCache", "-cacheRoot"};
        Configuration result = Configuration.fromCmdLine(args);
    }

    @Test
    public void testHarvestWithCache() {
        String[] args = {"-harvestWithCache"};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(false, false, false, false, null, false, true, null, result);
    }

    @Test
    public void testHarvestWithCacheWithRoot() {
        String path = "/tmp";
        String[] args = {"-harvestWithCache", "-cacheRoot", path};
        Configuration result = Configuration.fromCmdLine(args);
        assertConfiguration(false, false, false, false, null, false, true, path, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHarvestWithCacheMissingRoot() {
        String[] args = {"-harvestWithCache", "-cacheRoot"};
        Configuration result = Configuration.fromCmdLine(args);
    }

    private void assertConfiguration(boolean isHelp, boolean isVersion,
            boolean isUpdateMetadata,
            boolean isHarvestFromCache, String cachePath,
            boolean isHarvestToCache, boolean isHarvestWithCache,
            String cacheRoot,
            Configuration conf) {

        assertEquals("isHelp", isHelp, conf.isHelp());
        assertEquals("isVersion", isVersion, conf.isVersion());
        assertEquals("isUpdateMetadata", isUpdateMetadata, conf.isRegenerateMods());
        assertEquals("isHarvestFromCache", isHarvestFromCache, conf.isHarvestFromCache());
        assertEquals("cachePath", cachePath, conf.getCachePath());
        if (cacheRoot == null) {
            cacheRoot = Configuration.defaultCacheRoot();
        }
        assertEquals("cacheRoot", cacheRoot, conf.getCacheRoot());
        assertEquals("isHarvestToCache", isHarvestToCache, conf.isHarvestToCache());
        assertEquals("isHarvestWithCache", isHarvestWithCache, conf.isHarvestWithCache());
    }

}
