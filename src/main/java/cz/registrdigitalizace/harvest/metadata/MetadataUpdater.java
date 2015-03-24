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
package cz.registrdigitalizace.harvest.metadata;

import cz.registrdigitalizace.harvest.db.DaoException;
import cz.registrdigitalizace.harvest.db.DigObject;
import cz.registrdigitalizace.harvest.db.DigObjectDao;
import cz.registrdigitalizace.harvest.db.DigitizationRegistrySource;
import cz.registrdigitalizace.harvest.db.HarvestTransaction;
import cz.registrdigitalizace.harvest.db.IdSequence;
import cz.registrdigitalizace.harvest.db.IdSequenceDao;
import cz.registrdigitalizace.harvest.db.IterableResult;
import cz.registrdigitalizace.harvest.db.Library;
import cz.registrdigitalizace.harvest.db.Metadata;
import cz.registrdigitalizace.harvest.db.MetadataDao;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Computes metadata for hierarchies of digital objects.
 *
 * @author Jan Pokorsky
 */
public class MetadataUpdater {
    
    private static final Logger LOG = Logger.getLogger(MetadataUpdater.class.getName());
    private final DigitizationRegistrySource source;
    private long totalNumber;
    private long totalSize;

    public MetadataUpdater(DigitizationRegistrySource source) {
        this.source = source;
    }

    /**
     * Throws out all existing metadata (METADATA) of already harvested digital objects
     * and generates new.
     */
    public void regenerateDigObjects(Library library, ModsMetadataParser parser) throws DaoException {
        LOG.fine("start");
        DigObjectDao digObjectDao = new DigObjectDao();
        MetadataDao metadataDao = new MetadataDao();
        IdSequenceDao sequenceDao = new IdSequenceDao();
        HarvestTransaction transaction = new HarvestTransaction(source);
        digObjectDao.setDataSource(transaction);
        metadataDao.setDataSource(transaction);
        sequenceDao.setDataSource(transaction);
        boolean rollback = true;
        try {
            transaction.begin();
            IdSequence metadataSeq = sequenceDao.find(IdSequence.METADATA);
            metadataDao.delete(library.getId());
            IterableResult<DigObject> digObjects = digObjectDao.findMods(library);
            try {
                for (DigObject digObject; digObjects.hasNextResult();) {
                    digObject = digObjects.nextResult();
                    Metadata m = handleItem(digObject, parser);
                    metadataDao.insert(metadataSeq, m);
                    totalNumber++;
                    totalSize += digObject.getXml().length();
                }
            } finally {
                digObjects.close();
            }
            sequenceDao.update(metadataSeq);
            transaction.commit();
            rollback = false;
        } finally {
            if (rollback) {
                transaction.rollback();
            }
            transaction.close();
            LOG.fine("end");
        }
    }

    public long getTotalNumber() {
        return totalNumber;
    }

    public long getTotalSize() {
        return totalSize;
    }
    
    private Metadata handleItem(DigObject dobj, ModsMetadataParser parser) {
        Metadata dt;
        try {
            dt = parser.parse(new StringReader(dobj.getXml()));
            //            LOG.info(dobj.getXml() + "\n" + ModsMetadataParser.toString(dt));
        } catch (Exception e) {
            dt = new Metadata();
            LOG.log(Level.SEVERE, dobj.getId() + "\n" + dobj.getXml(), e);
        }
        dt.setDigObjId(dobj.getId());
        return dt;
    }
    
}
