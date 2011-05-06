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

package cz.registrdigitalizace.harvest;

import java.math.BigDecimal;
import java.util.List;

/**
 *
 * @author Jan Pokorsky
 */
public class HarvestedRecord {

    private BigDecimal id;
    private String uuid;
    private String type;
    private String descriptor;
    private boolean root;
    private transient List<String> childrenUuids;

    public BigDecimal getId() {
        return id;
    }

    public void setId(BigDecimal id) {
        this.id = id;
    }

    public boolean isRoot() {
        return root;
    }

    public void setRoot(boolean root) {
        this.root = root;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(String descriptor) {
        this.descriptor = descriptor;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public List<String> getChildren() {
        return childrenUuids;
    }

    public void setChildren(List<String> childrenUuids) {
        this.childrenUuids = childrenUuids;
    }

    @Override
    public String toString() {
        int length = descriptor == null ? 0 : descriptor.length();
        return String.format("HarvestedRecord[id:%s, uuid:%s, type:%s, root:%s, children:%s, xml.length:%s]",
                id, uuid, type, root, childrenUuids, length);
    }

}
