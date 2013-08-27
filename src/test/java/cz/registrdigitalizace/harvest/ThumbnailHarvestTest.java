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

import cz.registrdigitalizace.harvest.ThumbnailHarvest.ThumbnailSnapshot;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Jan Pokorsky
 */
public class ThumbnailHarvestTest {

    public ThumbnailHarvestTest() {
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
    public void testDownloadThumbnail() throws Exception {
        ThumbnailHarvest instance = new ThumbnailHarvest(null, null);
        URL url = new URL("http://kramerius.lib.cas.cz/search/img?pid=uuid:abae22d4-4611-11e1-1121-001143e3f55c&stream=IMG_THUMB&action=GETRAW&asFile=true");
        ThumbnailSnapshot snapshot = instance.downloadThumbnail(url);
        assertNotNull(snapshot);
        System.out.println("filename: " + snapshot.getFilename());
        assertEquals("ABA007000907941892000000001", snapshot.getFilename());
        InputStream content = snapshot.getContent();
        FileOutputStream out = new FileOutputStream(new File("/tmp/tn_snap.jpg"));
        for(int c = content.read(); c != -1; c = content.read()) {
            out.write(c);
        }
        out.close();
    }

    @Test
    public void testResolveFilename() {
        assertEquals("ABA007000907941892000000001", ThumbnailHarvest.resolveFilename("attachment; filename=ABA007000907941892000000001.jpg"));
        assertEquals("ABA007000907941892000000001", ThumbnailHarvest.resolveFilename("attachment; filename=ABA007000907941892000000001.jpg;"));
        assertEquals("ABA007000907941892000000001", ThumbnailHarvest.resolveFilename("attachment; filename=ABA007000907941892000000001"));
        assertEquals("ABA007000907941892000000001", ThumbnailHarvest.resolveFilename("attachment; filename=ABA007000907941892000000001.jpg; size=1"));
    }

}
