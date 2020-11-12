package tw.com.gamestudio.etl.agent.summary;

import com.fasterxml.jackson.databind.ObjectMapper;
import tw.com.gamestudio.etl.agent.api.SummaryPusher;
import tw.com.gamestudio.etl.agent.common.Configuration;
import tw.com.gamestudio.etl.agent.common.Summary;

/**
 * @author Game Studio
 */
public class ConsoleSummaryPusher implements SummaryPusher {

    private ObjectMapper mapper = new ObjectMapper();

    private String title;

    private String description;

    private boolean pretty = true;

    @Override
    public void init(Configuration configuration) throws Exception {

        // required
        title = configuration.get("title", null);
        if (title == null) {
            throw new Exception("missing required parameter 'title'");
        }

        // optional
        description = configuration.get("description", null);

        // optional
        pretty = configuration.get("pretty", true);

    }

    @Override
    public void push(Summary summary) {
        summary.setTitle(title);
        summary.setDescription(description);
        try {
            if (pretty) {
                System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(summary));
            } else {
                System.out.println(mapper.writeValueAsString(summary));
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

}