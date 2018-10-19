package edu.utarlington.pigeon.daemon.scheduler;

import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.Set;

/**
 * State storage for the Sparrow {@link Scheduler}. This is stored in its
 * own class since state storage may involve communicating to external services
 * such as ZooKeeper.
 */
public interface SchedulerState {
    /**
     * Initialize state storage. This should open connections to any external
     * services if required.
     * @throws IOException
     */
    public void initialize(Configuration conf) throws IOException;

    /**
     * Checks whether the given monitor address belongs to high priority workers
     * @param backendAddr
     * @return
     */
    public boolean isHW (InetSocketAddress backendAddr);

    /**
     * Signal that state storage will be queried for information about
     * {@code appId} in the future.
     */
    public boolean watchApplication(String appId);

    /**
     * Get the backends available for a particular application. Each backend includes a
     * resource vector giving current available resources. TODO: this might be changed
     * to include more detailed information per-node.
     *
     * For pigeon, this function also returned hw/lw backends separately
     */
    public Set<InetSocketAddress> getBackends(String appId, boolean isHW);
    public Set<InetSocketAddress> getBackends(boolean isHW);
}
