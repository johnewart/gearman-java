package net.johnewart.gearman.server.web;

import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import net.johnewart.gearman.server.config.GearmanServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class ClusterServlet extends HttpServlet {
    private final Logger LOG = LoggerFactory.getLogger(ClusterServlet.class);
    private static Configuration cfg = new Configuration();
    private final GearmanServerConfiguration serverConfiguration;

    public ClusterServlet(GearmanServerConfiguration serverConfiguration) {
        this.serverConfiguration = serverConfiguration;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        cfg.setClassForTemplateLoading(ClusterServlet.class, "templates");
        cfg.setTemplateUpdateDelay(0);

        try {
            final OutputStream output = resp.getOutputStream();
            OutputStreamWriter wr = new OutputStreamWriter(output);

            if(serverConfiguration.getCluster() != null) {
                cfg.getTemplate("cluster.ftl").process(new ClusterView(serverConfiguration.getCluster().getHazelcastInstance()), wr);
            } else {
                cfg.getTemplate("nocluster.ftl").process(null, wr);
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (TemplateException e) {
            e.printStackTrace();
        }

    }
}
