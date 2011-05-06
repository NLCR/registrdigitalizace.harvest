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

package cz.registrdigitalizace.harvest.oai;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Filtered stream to provide additional information about harvested data
 * like size of read bytes.
 *
 * @author Jan Pokorsky
 */
public final class HarvesterInputStream extends FilterInputStream {

    private long size;
    private long mark = -1;
    private int stack = 0;

    public HarvesterInputStream(InputStream in) {
        super(in);
    }

    public long getSize() {
        return size;
    }

    @Override
    public int read() throws IOException {
        boolean outsideCall = stack == 0;
        stack++;
        try {
            int read = super.read();
            if (outsideCall && read >= 0) {
                size++;
            }
            return read;
        } finally {
            stack--;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        boolean outsideCall = stack == 0;
        stack++;
        try {
            int read = super.read(b);
            if (outsideCall && read >= 0) {
                size += read;
            }
            return read;
        } finally {
            stack--;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        boolean outsideCall = stack == 0;
        stack++;
        try {
            int read = super.read(b, off, len);
            if (outsideCall && read >= 0) {
                size += read;
            }
            return read;
        } finally {
            stack--;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        boolean outsideCall = stack == 0;
        stack++;
        try {
            long skipped = super.skip(n);
            if (outsideCall && skipped >= 0) {
                size += skipped;
            }
            return skipped;
        } finally {
            stack--;
        }
    }

    @Override
    public synchronized void mark(int readlimit) {
        super.mark(readlimit);
        if (markSupported()) {
            mark = size;
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        super.reset();
        size = mark;
    }

}
