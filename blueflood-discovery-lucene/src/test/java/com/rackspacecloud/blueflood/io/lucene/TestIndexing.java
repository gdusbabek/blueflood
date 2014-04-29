package com.rackspacecloud.blueflood.io.lucene;

import com.google.common.collect.Lists;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import com.rackspacecloud.blueflood.utils.TimeValue;
import junit.framework.Assert;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class TestIndexing {
    
    private File dir;
    private LuceneDiscovery discovery;
    private static final Metric m0 = new Metric(Locator.createLocatorFromPathComponents("11111111", "rackspace.monitoring.entities.en22222222.checks.agent_memory.ch33333333.aaaa.bbbb.cccc.swap_free".split("\\.", -1)),
            99,
            0,
            new TimeValue(1, TimeUnit.DAYS),
            "pigeons");
    private static final Metric m1 = new Metric(Locator.createLocatorFromPathComponents("11111111", "rackspace.monitoring.entities.en99999999.checks.agent_memory.ch88888888.aaaa.bbbb.cccc.swap_free".split("\\.", -1)),
            500,
            0,
            new TimeValue(1, TimeUnit.DAYS),
            "cows");
    
    @Before
    public void createIndexDir() {
        dir = Utils.getRandomTempDir();
    }
    
    @After
    public void removeIndexDir() {
        Utils.removeDir(dir);
    }
    
    @Test
    public void testStandardIndex() throws Exception {
        System.out.println(dir.getAbsolutePath());
        discovery = new LuceneDiscovery(dir);
        discovery.insertDiscovery(Lists.newArrayList(m0, m1));
        discovery.flush();

        Assert.assertEquals(2, search("tenant", "11111111"));
        Assert.assertEquals(1, search("unit", "pigeons"));
        Assert.assertEquals(1, search("unit", "cows"));
        Assert.assertEquals(0, search("unit", "porcupines"));
        Assert.assertEquals(1, search("locator", LuceneDiscovery.makeLuceneKey(m0.getLocator())));
        Assert.assertEquals(1, search("locator", LuceneDiscovery.makeLuceneKey(m1.getLocator())));
    }
    
    @Test
    public void testRaxIndex() throws Exception {
        System.out.println(dir.getAbsolutePath());
        discovery = new RaxLuceneDiscovery(dir);
        discovery.insertDiscovery(Lists.newArrayList(m0, m1));
        discovery.flush();
        
        // first assertions are same as before.
        Assert.assertEquals(2, search("tenant", "11111111"));
        Assert.assertEquals(1, search("unit", "pigeons"));
        Assert.assertEquals(1, search("unit", "cows"));
        Assert.assertEquals(0, search("unit", "porcupines"));
        Assert.assertEquals(1, search("locator", LuceneDiscovery.makeLuceneKey(m0.getLocator())));
        Assert.assertEquals(1, search("locator", LuceneDiscovery.makeLuceneKey(m1.getLocator())));
        
        // rax specific
        Assert.assertEquals(1, search("check", "ch33333333"));
        Assert.assertEquals(1, search("check", "ch88888888"));
        Assert.assertEquals(0, search("check", "ch00000000"));
        Assert.assertEquals(1, search("entity", "en22222222"));
        Assert.assertEquals(1, search("entity", "en99999999"));
        Assert.assertEquals(0, search("entity", "en00000000"));
        Assert.assertEquals(2, search("name_field_0", "aaaa"));
        Assert.assertEquals(2, search("name_field_3", "swap_free"));
    }
    
    private int search(String field, String value) throws Exception {
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(dir)));
        QueryBuilder builder = new QueryBuilder(new StandardAnalyzer(Version.LUCENE_46));
        Query query = builder.createBooleanQuery(field, value, BooleanClause.Occur.MUST);
        //TermQuery query = new TermQuery(new Term(field, value));
        
        TopDocs docs = searcher.search(query, 100);
        return docs.totalHits;
    }
}
