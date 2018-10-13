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

    Set<InetSocketAddress> backends;
    private Configuration conf;

    @Override
    public void initialize(Configuration conf) throws IOException {
        backends = ConfigUtil.parseBackends(conf);
        this.conf = conf;
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
    public Set<InetSocketAddress> getBackends(String appId) {
        if (!appId.equals(conf.getString(PigeonConf.STATIC_APP_NAME))) {
            LOG.warn("Requested backends for app " + appId +
                    " but was expecting app " + conf.getString(PigeonConf.STATIC_APP_NAME));
        }
        return backends;
    }

}
