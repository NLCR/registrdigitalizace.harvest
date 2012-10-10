/*
 * Copyright (C) 2012 Jan Pokorsky
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
package cz.registrdigitalizace.harvest;

/**
 * Various helpers.
 *
 * @author Jan Pokorsky
 */
public final class Utils {

    public static String elapsedTime(long time) {
        long msecs = time % 1000;
        long secs = time % (1000 * 60) / 1000;
        long mins = time % (1000 * 60 * 60) / 1000 / 60;
        long hours = time % (1000 * 60 * 60 * 24) / 1000 / 60 / 60;
        return String.format("%02d:%02d:%02d.%03d", hours, mins, secs, msecs);
    }

}
