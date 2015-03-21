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

import cz.registrdigitalizace.harvest.HarvestedRecord;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stores harvested records in repository.
 * It is expected to {@link #init() init} the repository before first usage
 * and {@link #close() close} it after the last store operation.
 *
 * @author Jan Pokorsky
 */
public class RecordRepository {

    private static final Logger LOG = Logger.getLogger(RecordRepository.class.getName());

    private final DigObjectDao digiObjectDao;
    private final RelationDao relationDao;
    private final IdSequenceDao idSequenceDao;
    private final MetadataDao metadataDao;
    private final Library library;
    private final Date inputDate;
    private IdSequence metadataSequence;
    private IdSequence digiObjSequence;
    private IdSequence relationSequence;

    public RecordRepository(
            DigObjectDao digiObjectDao,
            RelationDao relationDao, IdSequenceDao idSequenceDao,
            MetadataDao metadataDao,
            Library library) {
        this.digiObjectDao = digiObjectDao;
        this.relationDao = relationDao;
        this.idSequenceDao = idSequenceDao;
        this.metadataDao = metadataDao;
        this.library = library;
        this.inputDate = new Date(System.currentTimeMillis());
    }

    /**
     * Makes snapshot of ID sequences.
     */
    public void init() throws DaoException {
        Map<String, IdSequence> ids = idSequenceDao.find(IdSequence.DIGOBJECT, IdSequence.METADATA, IdSequence.RELATION);
        digiObjSequence = getSequence(ids, IdSequence.DIGOBJECT);
        metadataSequence = getSequence(ids, IdSequence.METADATA);
        relationSequence = getSequence(ids, IdSequence.RELATION);
    }

    private static IdSequence getSequence(Map<String, IdSequence> ids, String sequenceName) {
        IdSequence s = ids.get(sequenceName);
        return (s != null) ? s : new IdSequence(sequenceName);
    }

    public void add(HarvestedRecord r) throws DaoException {
        try {
            upsertDigiobject(r);
            upsertRelations(r);
            upsertMetadata(r);
        } catch (Exception ex) {
            String details = r.toString();
            details += "\n\n" + r.getDescriptor();
            throw new DaoException(details, ex);
        }
    }

    public void remove(HarvestedRecord r) throws DaoException {
        BigDecimal digiObjId = digiObjectDao.find(library.getId(), r.getUuid());
        if (digiObjId == null) {
            LOG.log(Level.INFO, "Harvested record not found in DIGOBJEKT: {0}.\nLibrary: {1}",
                    new Object[]{r, library});
            return ;
        }
        r.setId(digiObjId);
        digiObjectDao.updateState(digiObjId, "deleted");

        Metadata metadata = new Metadata();
        metadata.setDigObjId(digiObjId);
        metadataDao.delete(metadata);

        // deleteOld relations
        relationDao.delete(r, library.getId());
    }

    public void close() throws DaoException {
        digiObjectDao.removeUnrelated(library.getId());
        upsertSequence(digiObjSequence);
        upsertSequence(metadataSequence);
        upsertSequence(relationSequence);
    }

    private void upsertSequence(IdSequence s) throws DaoException {
        if (s.isNew()) {
            idSequenceDao.insert(s);
        } else {
            idSequenceDao.update(s);
        }
    }
    private void upsertRelations(HarvestedRecord r) throws DaoException {
        // deleteOld old relations
        relationDao.delete(r, library.getId());
        relationDao.insert(relationSequence, r, library.getId());
    }

    private void upsertDigiobject(HarvestedRecord r) throws DaoException {
        BigDecimal id = digiObjectDao.find(library.getId(), r.getUuid());
        boolean isLeaf = r.getChildren().isEmpty();
        if (id == null) {
            id = digiObjSequence.increment();
            digiObjectDao.insert(library.getId(), id, r.getUuid(),
                    r.getType(), r.getDescriptor(), r.getFormat(), null, isLeaf);
        } else {
            digiObjectDao.update(id, r.getType(), r.getDescriptor(), r.getFormat(), isLeaf);
        }
        r.setId(id);
        r.getMetadata().setDigObjId(id);
    }
    
    private void upsertMetadata(HarvestedRecord r) throws DaoException {
        Metadata metadata = r.getMetadata();
        metadataDao.delete(metadata);
        metadataDao.insert(metadataSequence, metadata);
    }

}
