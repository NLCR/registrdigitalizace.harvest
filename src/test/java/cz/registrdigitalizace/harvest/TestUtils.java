/*
 * Copyright (C) 2015 Jan Pokorsky
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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import static org.junit.Assert.*;

/**
 * Helper class.
 *
 * @author Jan Pokorsky
 */
public class TestUtils {

    /**
     * Compares collections regardless of member order.
     */
    public static <T> void assertSetEquals(Collection<? extends T> expected, Collection<? extends T> result) {
        assertSetEquals(null, expected, result);
    }

    /**
     * Compares collections regardless of member order.
     */
    public static <T> void assertSetEquals(String msg, Collection<? extends T> expected, Collection<? extends T> result) {
        if (result == null || expected == null) {
            assertEquals(msg, expected, result);
        } else {
            assertEquals(msg, expected instanceof Set ? expected : new HashSet<T>(expected),
                    result instanceof Set ? result : new HashSet<T>(result));
        }
    }

    public static <T> Set<T> asSet(T... t) {
        return new HashSet<T>(Arrays.asList(t));
    }

}
