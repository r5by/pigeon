package edu.utarlington.pigeon.daemon.util;

import edu.utarlington.pigeon.daemon.PigeonConf;
import org.apache.commons.configuration.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/** Utilities for interrogating system resources. */
public class Resources {
    public static int getSystemMemoryMb(Configuration conf) {
        int systemMemory = -1;
        try {
            Process p = Runtime.getRuntime().exec("cat /proc/meminfo");
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = in.readLine();
            while (line != null) {
                if (line.contains("MemTotal")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length > 1) {
                        int memory = Integer.parseInt(parts[1]) / 1000;
                        systemMemory = memory;
                    }
                }
                line = in.readLine();
            }
        } catch (IOException e) {}
        if (conf.containsKey(PigeonConf.SYSTEM_MEMORY)) {
            return conf.getInt(PigeonConf.SYSTEM_MEMORY);
        } else {
            if (systemMemory != -1) {
                return systemMemory;
            } else {
                return PigeonConf.DEFAULT_SYSTEM_MEMORY;
            }
        }
    }

    public static int getSystemCPUCount(Configuration conf) {
        // No system interrogation yet
        return conf.getInt(PigeonConf.SYSTEM_CPUS, PigeonConf.DEFAULT_SYSTEM_CPUS);
    }
}
