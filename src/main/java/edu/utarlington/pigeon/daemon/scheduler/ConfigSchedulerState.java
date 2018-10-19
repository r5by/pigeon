package edu.utarlington.pigeon.daemon.scheduler;

import edu.utarlington.pigeon.daemon.PigeonConf;
import edu.utarlington.pigeon.daemon.util.ConfigUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Scheduler state that operates based on a static configuration file.
 */
public class ConfigSchedulerState implements SchedulerState{
    private static final Logger LOG = Logger.getLogger(ConfigSchedulerState.class);

    /* Pigeon scheduler save backends for high/low priority tasks separately */
    Set<InetSocketAddress> backendsHW;
    Set<InetSocketAddress> backendsLW;
    private Configuration conf;

    @Override
    public void initialize(Configuration conf) throws IOException {
        backendsHW = ConfigUtil.parseBackends(conf, PigeonConf.STATIC_NODE_MONITORS_HW);
        backendsLW = ConfigUtil.parseBackends(conf, PigeonConf.STATIC_NODE_MONITORS_LW);
        this.conf = conf;
    }

    @Override
    public boolean isHW(InetSocketAddress backendAddr) {
        return backendsHW.contains(backendAddr);
    }


    @Override
    public boolean watchApplication(String appId) {
        if (!appId.equals(conf.getString(PigeonConf.STATIC_APP_NAME))) {
            LOG.warn("Requested watch for app " + appId +
                    " but was expecting app " + conf.getString(PigeonConf.STATIC_APP_NAME));
        }
        return true;
    }

    @Override
    public Set<InetSocketAddress> getBackends(String appId, boolean isHW) {
        if (!appId.equals(conf.getString(PigeonConf.STATIC_APP_NAME))) {
            LOG.warn("Requested backends for app " + appId +
                    " but was expecting app " + conf.getString(PigeonConf.STATIC_APP_NAME));
        }

        return isHW ? backendsHW : backendsLW;
    }

    @Override
    public Set<InetSocketAddress> getBackends(boolean isHW) {
        if(isHW)
            LOG.debug("Preparing backends for hight priority workloads");
        else
            LOG.debug("Preparing backends for low priority workloads");

        return isHW ? backendsHW : backendsLW;
    }
}
