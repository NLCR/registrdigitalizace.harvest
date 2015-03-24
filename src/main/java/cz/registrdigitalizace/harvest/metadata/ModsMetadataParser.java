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

import cz.registrdigitalizace.harvest.db.Metadata;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.util.JAXBResult;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/**
 * Parses MODS metadata of digital object. It uses XSL transformation
 * to {@link DigobjectType digobject} schema
 * in order to simplify customization for different libraries content
 * at some time in the future. For now it is optimized for NDK and K4 MODS.
 *
 * @author Jan Pokorsky
 */
public final class ModsMetadataParser {
    
    public static final String STYLESHEET =  "mods3-metadata.xsl";
    private static final Logger LOG = Logger.getLogger(ModsMetadataParser.class.getName());
    
    private Transformer transformer;
    private Unmarshaller digObjReader;

    public static String getDefaultXslt() {
        URL xslt = ModsMetadataParser.class.getResource(STYLESHEET);
        if (xslt == null) {
            throw new IllegalStateException("Missing stylesheet " + STYLESHEET);
        }
        return xslt.toExternalForm();
    }

    public ModsMetadataParser(String stylesheet) {
        try {
            initUnmarshaler();
            initTransformer(stylesheet);
        } catch (TransformerConfigurationException ex) {
            throw new IllegalStateException(ex);
        } catch (JAXBException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void initUnmarshaler() throws JAXBException {
        JAXBContext jaxbCtx = JAXBContext.newInstance(Metadata.class);
        digObjReader = jaxbCtx.createUnmarshaller();
    }

    private void initTransformer(String stylesheet) throws TransformerConfigurationException {
        TransformerFactory tfactory = TransformerFactory.newInstance();
        String systemId = stylesheet;
        if (STYLESHEET.equals(stylesheet)) {
            URL xsltRes = ModsMetadataParser.class.getResource(stylesheet);
            if (xsltRes == null) {
                throw new TransformerConfigurationException("Missing stylesheet.");
            }
            systemId = xsltRes.toExternalForm();
        }
        transformer = tfactory.newTransformer(new StreamSource(systemId));
    }
    
    public Metadata parse(InputStream modsxml) {
        return parse(new StreamSource(modsxml));
    }
    
    public Metadata parse(Reader modsxml) {
        return parse(new StreamSource(modsxml));
    }

    public Metadata parse(Source src) {
        try {
            JAXBResult result = new JAXBResult(digObjReader);
            if (LOG.isLoggable(Level.FINE)) {
                StringWriter buffer = new StringWriter();
                transformer.transform(src, new StreamResult(buffer));
                dumpTransformedXml(buffer.toString(), result, Level.FINE);
            } else {
                transformer.transform(src, result);
            }
            Metadata dobj = (Metadata) result.getResult();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.valueOf(dobj));
            }
            return dobj;
        } catch (TransformerException ex) {
            throw new IllegalStateException(ex);
        } catch (JAXBException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * helper to log transformed XML and copy contents to original Result
     */
    private static void dumpTransformedXml(String xml, Result result, Level level) {
        try {
            TransformerFactory tfactory = TransformerFactory.newInstance();
            Transformer t = tfactory.newTransformer();
            t.transform(new StreamSource(new StringReader(xml)), result);
            LOG.log(level, xml);
        } catch (TransformerException ex) {
            throw new IllegalStateException(ex);
        }
    }

}
