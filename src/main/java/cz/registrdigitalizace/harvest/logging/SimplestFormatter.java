/*
 * Copyright (C) 2013 Jan Pokorsky
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cz.registrdigitalizace.harvest.logging;

import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * No stack traces.
 *
 * @author Jan Pokorsky
 */
public class SimplestFormatter extends Formatter {

    Date dat = new Date();
    private final static String format = "{0,date} {0,time}";
    private MessageFormat formatter;
    private Object args[] = new Object[1];

    /**
     * Format the given LogRecord.
     *
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public synchronized String format(LogRecord record) {
        String lineSeparator = System.getProperty("line.separator");
        StringBuilder sb = new StringBuilder();
        dat.setTime(record.getMillis());
        args[0] = dat;
        StringBuffer text = new StringBuffer();
        if (formatter == null) {
            formatter = new MessageFormat(format);
        }
        formatter.format(args, text, null);
        sb.append(text);
        sb.append(" ");
        if (record.getSourceClassName() != null) {
            sb.append(record.getSourceClassName());
        } else {
            sb.append(record.getLoggerName());
        }
        if (record.getSourceMethodName() != null) {
            sb.append(" ");
            sb.append(record.getSourceMethodName());
        }
        sb.append(lineSeparator);
        String message = formatMessage(record);
        sb.append(record.getLevel().getLocalizedName());
        sb.append(": ");
        sb.append(message);
        if (record.getThrown() != null) {
            sb.append(": ");
            sb.append(record.getThrown().getClass().getName());
        }
        sb.append(lineSeparator);
        return sb.toString();
    }
}
