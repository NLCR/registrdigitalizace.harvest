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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Helper class to implement iterable {@link ResultSet}.
 * Implementors are expected to supply {@link #fetchNext() } content.
 * Clients are expected to invoke {@link #close() } as they are done.
 *
 * @author Jan Pokorsky
 */
public abstract class IterableResult<T> implements Iterable<T>, Iterator<T> {

    protected final ResultSet rs;
    private final Statement stmt;
    private boolean lastNext = false;

    public IterableResult(Statement stmt, ResultSet rs) {
        this.rs = rs;
        this.stmt = stmt;
    }

    @Override
    public final Iterator<T> iterator() {
        return this;
    }

    @Override
    public final boolean hasNext() {
        try {
            return hasNextResult();
        } catch (DaoException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public final T next() {
        try {
            return nextResult();
        } catch (DaoException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException("Not supported.");
    }

    public final boolean hasNextResult() throws DaoException {
        try {
            lastNext = rs.next();
            return lastNext;
        } catch (SQLException ex) {
            throw new DaoException(ex);
        }
    }

    public final T nextResult() throws DaoException, NoSuchElementException {
        if (!lastNext && !hasNextResult()) {
            throw new NoSuchElementException();
        }
        return fetchNext();
    }
    
    protected abstract T fetchNext() throws DaoException;

    public final void close() {
        SQLQuery.tryClose(rs);
        SQLQuery.tryClose(stmt);
    }

}
