package net.johnewart.gearman.server.web;

import freemarker.template.Configuration;
import freemarker.template.TemplateException;
import net.johnewart.gearman.engine.storage.ExceptionStorageEngine;
import net.johnewart.gearman.engine.storage.NoopExceptionStorageEngine;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class ExceptionsServlet extends HttpServlet {
    private final ExceptionStorageEngine exceptionStorageEngine;
    private static Configuration cfg = new Configuration();

    public ExceptionsServlet(ExceptionStorageEngine exceptionStorageEngine) {
        this.exceptionStorageEngine = exceptionStorageEngine;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        cfg.setClassForTemplateLoading(ClusterServlet.class, "templates");
        cfg.setTemplateUpdateDelay(0);

        final int pageSize, pageNum;
        if(req.getParameter("pageSize") != null) {
            pageSize = Integer.valueOf(req.getParameter("pageSize")).intValue();
        } else {
            pageSize = 50;
        }

        if(req.getParameter("pageNum") != null) {
            pageNum = Integer.valueOf(req.getParameter("pageNum")).intValue();
        } else {
            pageNum = 1;
        }

        try {
            final OutputStream output = resp.getOutputStream();
            OutputStreamWriter wr = new OutputStreamWriter(output);

            if(exceptionStorageEngine.getClass() != NoopExceptionStorageEngine.class) {
                cfg.getTemplate("exceptions.ftl").process(new ExceptionsView(exceptionStorageEngine, pageSize, pageNum), wr);
            } else {
                cfg.getTemplate("noexceptions.ftl").process(null, wr);
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        } catch (TemplateException e) {
            e.printStackTrace();
        }

    }

}
