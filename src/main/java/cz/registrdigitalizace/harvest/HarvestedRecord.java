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

import cz.registrdigitalizace.harvest.db.Metadata;
import java.math.BigDecimal;
import java.util.List;

/**
 * OAI record.
 *
 * @author Jan Pokorsky
 */
public class HarvestedRecord {

    private BigDecimal id;
    private String uuid;
    private String type;
    private String descriptor;
    private String format;
    private Metadata metadata;
    @Deprecated
    private boolean root;
    private transient List<String> childrenUuids;

    /**
     * Gets ID of persisted record or {@code null} if not yet persisted.
     * @return ID
     */
    public BigDecimal getId() {
        return id;
    }

    /**
     * Sets ID of persisted record.
     */
    public void setId(BigDecimal id) {
        this.id = id;
    }

    /**
     * Signals root record of the harvested hierarchy.
     * @deprecated It is no more necessary as RelationDao consider each
     *      harvested record as root and DigitalObject.removeUnrelated
     *      clears false roots when all records are available from OAI responses.
     */
    @Deprecated
    public boolean isRoot() {
        return root;
    }

    /** @see #isRoot */
    @Deprecated
    public void setRoot(boolean root) {
        this.root = root;
    }

    /**
     * Gets meta data content as harvested.
     */
    public String getDescriptor() {
        return descriptor;
    }

    /**
     * Sets meta data content as harvested.
     */
    public void setDescriptor(String descriptor) {
        this.descriptor = descriptor;
    }

    /**
     * Gets metada data content format. E.g. XML namespace
     */
    public String getFormat() {
        return format;
    }

    /**
     * Sets metada data content format. E.g. XML namespace
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * Gets parsed selected meta data.
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Sets parsed selected meta data.
     */
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Gets document type (PERIODICAL, ...).
     * @return
     */
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

    /**
     * Gets UUIDs of harvested child relations.
     * @return list of UUIDs
     */
    public List<String> getChildren() {
        return childrenUuids;
    }

    /**
     * Sets UUIDs of harvested child relations.
     * @param childrenUuids  list of UUIDs
     */
    public void setChildren(List<String> childrenUuids) {
        this.childrenUuids = childrenUuids;
    }

    @Override
    public String toString() {
        int length = descriptor == null ? 0 : descriptor.length();
        return String.format("HarvestedRecord[id:%s, uuid:%s, type:%s, root:%s"
                + ", children:%s, xml.length:%s, format:%s,\n  %s]",
                id, uuid, type, root, childrenUuids, length, format, metadata);
    }

}
