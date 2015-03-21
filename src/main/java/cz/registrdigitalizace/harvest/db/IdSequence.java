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

import java.math.BigDecimal;

/**
 *
 * @author Jan Pokorsky
 */
public class IdSequence {

    public static final String DIGOBJECT = "cz.incad.rd.DigObjekt";
    public static final String RELATION = "cz.incad.rd.DigVazby";
    public static final String METADATA = "cz.incad.rd.Metadata";

    private BigDecimal lastId;
    private final String sequenceName;
    private final boolean memory;

    public IdSequence(String sequenceName) {
        this(BigDecimal.ZERO, sequenceName, true);
    }

    public IdSequence(BigDecimal lastId, String sequenceName) {
        this(lastId, sequenceName, false);
    }
    
    private IdSequence(BigDecimal lastId, String sequenceName, boolean memory) {
        if (lastId == null || sequenceName == null) {
            throw new IllegalArgumentException(String.format("id: %s, name: %s", lastId, sequenceName));
        }
        this.lastId = lastId;
        this.sequenceName = sequenceName;
        this.memory = memory;
    }

    public BigDecimal increment() {
        lastId = lastId.add(BigDecimal.ONE);
        return lastId;
    }

    public BigDecimal getId() {
        return lastId;
    }

    public String getName() {
        return sequenceName;
    }

    /**
     * return {@code true} when sequence has not been persisted yet.
     */
    boolean isNew() {
        return memory;
    }

    @Override
    public String toString() {
        return String.format("IdSequence[%s, %s, %s]", lastId, sequenceName, memory);
    }

}
