/*
 * PIGEON
 * Copyright 2018 Univeristy of Texas at Arlington
 *
 * Modified from Sparrow - University of California, Berkeley
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package edu.utarlington.pigeon.daemon.util;

import edu.utarlington.pigeon.thrift.TTaskLaunchSpec;
import edu.utarlington.pigeon.thrift.TTaskSpec;

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

    /**
     * Calc the average of a given double list
     * @param list
     * @return
     */
    public static double mean(List<TTaskLaunchSpec> list) {
        List<Double> tasks = new ArrayList<Double>();
        double sum = 0.0;
        for (double value : tasks
             ) {
            sum += value;
        }
        double mean = sum / list.size();
        return mean;
    }
}
