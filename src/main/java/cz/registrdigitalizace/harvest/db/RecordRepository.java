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
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jan Pokorsky
 */
public class RecordRepository {

    private static final Logger LOG = Logger.getLogger(RecordRepository.class.getName());

    private final LocationDao locationDao;
    private final DigObjectDao digiObjectDao;
    private final RelationDao relationDao;
    private final IdSequenceDao idSequenceDao;
    private final Library library;
    private final Date inputDate;
    private IdSequence locationSequence;
    private IdSequence digiObjSequence;
    private IdSequence relationSequence;

    public RecordRepository(
            LocationDao locationDao, DigObjectDao digiObjectDao,
            RelationDao relationDao, IdSequenceDao idSequenceDao,
            Library library) {
        this.locationDao = locationDao;
        this.digiObjectDao = digiObjectDao;
        this.relationDao = relationDao;
        this.idSequenceDao = idSequenceDao;
        this.library = library;
        this.inputDate = new Date(System.currentTimeMillis());
    }

    public void init() throws DaoException {
        Map<String, IdSequence> ids = idSequenceDao.find(IdSequence.DIGOBJECT, IdSequence.LOCATION, IdSequence.RELATION);
        digiObjSequence = getSequence(ids, IdSequence.DIGOBJECT);
        locationSequence = getSequence(ids, IdSequence.LOCATION);
        relationSequence = getSequence(ids, IdSequence.RELATION);
    }

    private static IdSequence getSequence(Map<String, IdSequence> ids, String sequenceName) {
        IdSequence s = ids.get(sequenceName);
        return (s != null) ? s : new IdSequence(sequenceName);
    }

    public void add(HarvestedRecord r) throws DaoException {
        upsertDigiobject(r);
        upsertLocation(r);
        upsertRelations(r);
    }

    public void remove(HarvestedRecord r) throws DaoException {
        BigDecimal digiObjId = digiObjectDao.find(r.getUuid());
        if (digiObjId == null) {
            LOG.log(Level.INFO, "Harvested record not found in DIGIOBJEKT: {0}.\nLibrary: {1}",
                    new Object[]{r, library});
            return ;
        }
        r.setId(digiObjId);
        // deleteOld locations; XXX try to deleteOld all obsolete locations with one query like digi objects
        BigDecimal locationId = locationDao.find(digiObjId, library.getId());
        if (locationId != null) {
            locationDao.delete(Arrays.asList(locationId));
        }
        // deleteOld relations
        relationDao.delete(r, library.getId());
    }

    public void close() throws DaoException {
        digiObjectDao.removeUnrelated();
        upsertSequence(digiObjSequence);
        upsertSequence(locationSequence);
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

    private void upsertLocation(HarvestedRecord r) throws DaoException {
        BigDecimal id = locationDao.find(r.getId(), library.getId());
        if (id == null) {
            if (r.getChildren().isEmpty()) {
                // insertOld location only for leafs
                id = locationSequence.increment();
                locationDao.insert(id, r.getId(), library.getId(), inputDate);
            }
        } else {
            if (r.getChildren().isEmpty()) {
                locationDao.update(id, inputDate);
            } else {
                locationDao.delete(Arrays.asList(id));
            }
        }
    }

    private void upsertDigiobject(HarvestedRecord r) throws DaoException {
        BigDecimal id = digiObjectDao.find(r.getUuid());
        if (id == null) {
            id = digiObjSequence.increment();
            digiObjectDao.insert(id, r.getUuid(), r.getType(), r.getDescriptor());
        } else {
            digiObjectDao.update(id, r.getType(), r.getDescriptor());
        }
        r.setId(id);
    }

}
