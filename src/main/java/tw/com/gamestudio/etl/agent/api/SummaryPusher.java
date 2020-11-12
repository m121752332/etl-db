package tw.com.gamestudio.etl.agent.api;

import tw.com.gamestudio.etl.agent.common.Configuration;
import tw.com.gamestudio.etl.agent.common.Summary;

/**
 * @author Game Studio
 */
public interface SummaryPusher {

    /**
     * Initialize the summary pusher.
     *
     * @param configuration The configuration
     * @throws Exception If the configuration is not valid
     */
    void init(Configuration configuration) throws Exception;

    /**
     * Push the summary.
     *
     * @param summary The summary
     */
    void push(Summary summary);

}
