package com.rackspacecloud.blueflood.io.lucene;

import com.google.common.base.Ticker;
import com.rackspacecloud.blueflood.io.DiscoveryIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LuceneDiscovery implements DiscoveryIO {
    
    private static final String PRIMARY_KEY_FIELD = "locator_hash";
    
    
    private final Ticker ticker = Ticker.systemTicker();
    private final File indexDir;
    private final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
    
    private long lastFlush = ticker.read();
    
    private int flushAtSize = 50000;
    private long flushAtNanos= 300 * 1000000000; // 5 minutes.
    
    private ReadWriteLock flushLock = new ReentrantReadWriteLock(true);
    private Map<Locator, Metric> unflushed = new HashMap<Locator, Metric>();
    
    public LuceneDiscovery(File indexDir) {
        this.indexDir = indexDir;
    }
    
    @Override
    public void insertDiscovery(List<Metric> metrics) throws Exception {
        flushLock.readLock().lock();
        try {
            for (Metric m : metrics) {
                unflushed.put(m.getLocator(), m);
            }
        } finally {
            flushLock.readLock().unlock();
        }
        maybeFlush();
    }
    
    public void flush() throws IOException {
        Map<Locator, Metric> flushMap = new HashMap<Locator, Metric>();
        flushLock.writeLock().lock();
        try {
            flushMap.putAll(unflushed);
            unflushed.clear();
        } finally {
            flushLock.writeLock().unlock();
        }
        
        flushAtNanos = ticker.read();
        
        if (flushMap.size() == 0)
            return;
        
        Map<String, Document> documents = new HashMap<String, Document>();
        for (Metric metric : flushMap.values()) {
            Document doc = new Document();
            String locatorString = makeLuceneKey(metric.getLocator());
            String indexKey = Integer.toHexString(locatorString.hashCode());
            doc.add(new Field(PRIMARY_KEY_FIELD, indexKey, TextField.TYPE_NOT_STORED));
            doc.add(new Field("locator", locatorString, TextField.TYPE_STORED));
            doc.add(new Field("tenant", metric.getLocator().getTenantId(), TextField.TYPE_NOT_STORED));
            doc.add(new Field("datatype", metric.getDataType() == null ? "unknown" : metric.getDataType().toString(), TextField.TYPE_NOT_STORED));
            doc.add(new Field("unit", metric.getUnit() == null ? "unknown" : metric.getUnit(), TextField.TYPE_NOT_STORED));
            doc.add(new Field("rolluptype", metric.getRollupType().name(), TextField.TYPE_NOT_STORED));
            amendDocument(doc, metric);
            
            documents.put(indexKey, doc);
        }
        
        Directory directory = FSDirectory.open(indexDir);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter writer = new IndexWriter(directory, config);
        deleteDocuments(documents.keySet(), writer);
        writer.addDocuments(documents.values());
        writer.commit();
        writer.forceMerge(1);
        writer.waitForMerges();
        writer.close();
        
    }
    
    private static void deleteDocuments(Collection<String> indexKeys, IndexWriter writer) throws IOException {
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, false));
        QueryParser parser = new QueryParser(Version.LUCENE_46, PRIMARY_KEY_FIELD, writer.getAnalyzer());
        for (String indexKey : indexKeys) {
            try {
                Query query = parser.parse(indexKey);
                TopDocs docs = searcher.search(query, 1);
                if (docs.totalHits > 0) {
                    int doc = docs.scoreDocs[0].doc;
                    writer.tryDeleteDocument(searcher.getIndexReader(), doc);
                }
            } catch (ParseException ex) {
                throw new IOException(ex);
            }
        }
        
        if (writer.hasDeletions()) {
            writer.forceMergeDeletes(true);
        }
    }
    
    // overwrite in child class.
    public void amendDocument(Document doc, Metric metric) {}
    
    private void maybeFlush() throws IOException {
        if (unflushed.size() > flushAtSize || ticker.read() - lastFlush > flushAtNanos)
            flush();
    }
    
    // swap out characters lucene doesn't like.
    public static final String makeLuceneKey(Locator locator) {
        String s = locator.toString();
        s = s.replace(':', '_');
        return s;
    }
}
