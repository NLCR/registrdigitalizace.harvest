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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import org.openarchives.oai2.OAIPMHerrorType;
import org.openarchives.oai2.OAIPMHerrorcodeType;
import org.openarchives.oai2.OAIPMHtype;

/**
 * The client implementation of the OAI protocol.
 *
 * @author Jan Pokorsky
 */
public class Harvester {
    private static final Logger LOG = Logger.getLogger(Harvester.class.getName());

    private final OaiSource oaiSource;
    private final XmlContext xmlCtx;

    public Harvester(OaiSource oaiSource, XmlContext xmlCtx) {
        this.oaiSource = oaiSource;
        this.xmlCtx = xmlCtx;
    }

    public ListResult<Record> getListRecords(boolean listContentStreaming) throws OaiException {
        try {
            InputStream stream = oaiSource.openConnection();
            OaiParser parser = createParser(xmlCtx, listContentStreaming);

            parser.parse(stream);
            OAIPMHtype oaiType = parser.getOaiType();
            List<OAIPMHerrorType> errors = oaiType.getError();
            if (errors != null && !errors.isEmpty()) {
                OAIPMHerrorType error = errors.get(0);
                if (error.getCode() != OAIPMHerrorcodeType.NO_RECORDS_MATCH) {
                    throw new OaiException(errors);
                }
            }
            ListResult<Record> listResult = new ListResult<Record>(parser, oaiSource, Record.class);
            return listResult;
        } catch (XMLStreamException ex) {
            LOG.log(Level.INFO, "  chyba parsovani1: " + ex);
            throw new OaiException(ex, oaiSource.getUrl());
        } catch (JAXBException ex) {
            LOG.log(Level.INFO, "  chyba parsovani2: " + ex);
            throw new OaiException(ex, oaiSource.getUrl());
        } catch (IOException ex) {
            LOG.log(Level.INFO, "  chyba parsovani3: " + ex);
            throw new OaiException(ex, oaiSource.getUrl());
        } catch (OaiException ex) {
            LOG.log(Level.INFO, "  chyba parsovani4: " + ex);
            ex.setSource(oaiSource.getUrl());
            throw ex;
        }
    }
    
    OaiParser createParser(XmlContext xmlCtx, boolean listContentStreaming) {
        OaiParser parser = new OaiParser(xmlCtx, listContentStreaming);
        return parser;
    }

}
