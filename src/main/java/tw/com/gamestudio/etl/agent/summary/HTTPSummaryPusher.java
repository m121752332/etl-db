package tw.com.gamestudio.etl.agent.summary;

import com.fasterxml.jackson.databind.ObjectMapper;
import tw.com.gamestudio.etl.agent.api.SummaryPusher;
import tw.com.gamestudio.etl.agent.common.Configuration;
import tw.com.gamestudio.etl.agent.common.Summary;

import java.io.BufferedOutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;

/**
 * @author Game Studio
 */
public class HTTPSummaryPusher implements SummaryPusher {

    private ObjectMapper mapper = new ObjectMapper();

    private String url;

    private int connectTimeout;

    private int readTimeout;

    private String encoding;

    private String proxy;

    private String username;

    private String password;

    @Override
    public void init(Configuration configuration) throws Exception {

        // required
        url = configuration.get("url", null);
        if (url == null) {
            throw new Exception("missing required parameter 'url'");
        }

        // optional
        connectTimeout = configuration.get("connect_timeout", 10 * 1000);

        // optional
        readTimeout = configuration.get("read_timeout", 30 * 1000);

        // optional
        encoding = configuration.get("encoding", "UTF-8");

        // optional
        proxy = configuration.get("proxy", null);

        // optional
        username = configuration.get("username", null);

        // optional
        password = configuration.get("password", null);

    }

    @Override
    public void push(Summary summary) {
        BufferedOutputStream out = null;
        try {
            String date = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss Z").format(new Date());
            //String content = mapper.writeValueAsString(summary);
            String content = null;
            URL url = new URL(this.url);
            Proxy proxy = Proxy.NO_PROXY;
            if (this.proxy != null && !this.proxy.isEmpty()) {
                URL proxyUrl = new URL(this.proxy);
                proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyUrl.getHost(), proxyUrl.getPort()));
            }
            HttpURLConnection connection = (HttpURLConnection) url.openConnection(proxy);
            connection.setRequestMethod("POST");
            connection.setConnectTimeout(connectTimeout);
            connection.setReadTimeout(readTimeout);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Length", "" + content.length());
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Date", date);
            if (username != null && password != null) {
                connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes("UTF-8")));
            }
            connection.setInstanceFollowRedirects(true);
            out = new BufferedOutputStream(connection.getOutputStream());
            out.write(content.getBytes(encoding));
            out.flush();
            int status = connection.getResponseCode();
            System.out.println(status);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }
        }
    }
}