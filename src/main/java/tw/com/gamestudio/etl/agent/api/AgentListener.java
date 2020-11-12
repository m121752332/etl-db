package tw.com.gamestudio.etl.agent.api;

import tw.com.gamestudio.etl.agent.common.Row;
import tw.com.gamestudio.etl.agent.common.Table;

/**
 * @author Game Studio
 */
public interface AgentListener {

    /**
     * Called before any processing is started.
     */
    void onStart();

    /**
     * Called after all processing has ended.
     */
    void onEnd();

    /**
     * Called when a table is discovered.
     *
     * @param table The table
     */
    void onTable(Table table);

    /**
     * Called when a row is discovered.
     *
     * @param table The table
     * @param row The row
     */
    void onRow(Table table, Row row);

}
