package edu.utarlington.pigeon.daemon.util;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple util kit to impl. Pigeon logics in manipulate lists
 */
public class ListUtils {

    /**
     * Evenly split a list of available master nodes, used by Pigeon scheduler to
     * dispatch tasks, refer to Pigeon paper Section 2.2 for details.
     */
    public static <T> List<List<T>> split(List<T> list, final int Ng) {
        List<List<T>> parts = new ArrayList<List<T>>();

        final int F = list.size();
        final int x = F/Ng;
        final int r = F%Ng;

        for (int i = 0; i < (Ng - r) * x; i +=x) {
            parts.add(new ArrayList<T>(
                    list.subList(i, i + x)
            ));
        }

        for (int j = (Ng - r) * x; j < F; j += x + 1) {
            parts.add(new ArrayList<T>(
                    list.subList(j, j+x+1)
            ));
        }

        return parts;
    }
}
