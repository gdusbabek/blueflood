package com.rackspacecloud.blueflood.dw.query;

import com.codahale.metrics.annotation.Timed;
import com.rackspacecloud.blueflood.io.lucene.LuceneDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

@Path("/exp/lucene")
@Produces(MediaType.APPLICATION_JSON)
public class LuceneResource {
    private static final Logger log = LoggerFactory.getLogger(LuceneResource.class);
    
    private final LuceneDiscovery lucene = new LuceneDiscovery();

    @GET
    @Timed
    @Path("find")
    public ListResponse find(@QueryParam("filter") String filter) {
        if (filter.equals("*")) {
            filter = ".*";
        }
        try {
            Collection<String> allResults = lucene.getAllLocators();
            List<String> hits = new ArrayList<String>();
            for (String s : allResults) {
                try {
                    if (s.matches(filter)) {
                        hits.add(s);
                    }
                } catch (Throwable th) {
                    // doesn't match. remember. we don't validate input.
                }
            }
            return new ListResponse(hits);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new WebApplicationException(ex, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
}
