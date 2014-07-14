package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Migration2 {
    
    private static final String SAME = "SAME".intern();
    private static final String RENEW = "RENEW".intern();
    private static final String NONE = "NONE".intern();
    private static final String DO_NOT_RESUME = "DO NOT RESUME".intern();
    private static final int CONCURRENCY_FACTOR = 2;
    private static final int ADDITIONAL_CONNECTIONS_PER_HOST = 4;
    
    private static final PrintStream out = System.out;
    
    private static final Options cliOptions = new Options();
    
    private static final String FILE = "file";
    
    private static final String DST_CLUSTER = "dcluster";
    private static final String DST_KEYSPACE = "dkeyspace";
    private static final String DST_CF = "dfamily";
    private static final String DST_VERSION = "dver";
    
    private static final String SRC_CLUSTER = "scluster";
    private static final String SRC_KEYSPACE = "skeyspace";
    private static final String SRC_CF = "sfamily";
    private static final String SRC_VERSION = "sver";
    
    private static final String FROM = "from";
    private static final String TO = "to";
    
    private static final String MAX_ROWS = "rows";
    private static final String TTL_SECONDS = "ttl";
    private static final String RATE_PER_MINUTE = "rate";
    
    private static final String CONCURRENCY = "concurrency";
    private static final String RESUME = "resume";
    
    static {
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withDescription("Location of locator file").create(FILE));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withValueSeparator(',').withDescription("comma delimited list of host:port of cassandra cluster to write to").create(DST_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withValueSeparator(',').withDescription("comma delimited list of host:port of cassandra cluster to read from").create(SRC_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) of when to start migrating data. defaults to one year ago.").create(FROM));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) Datetime of when to stop migrating data. defaults to right now.").create(TO));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Destination keyspace (default=data)").create(DST_KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withLongOpt("[optional] Destination column family to migrate").create(DST_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Destination column family (default=metrics_1440m").create(DST_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Destination cassandra version (default=2.0)").create(DST_VERSION));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Source keyspace (default=DATA)").create(SRC_KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withLongOpt("[optional] Source column family to migrate").create(SRC_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Source column family (default=metrics_1440m").create(SRC_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Source cassandra version (default=1.0)").create(SRC_VERSION));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Maximum number of rows to copy (default=INFINITY)").create(MAX_ROWS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] TTL in seconds for copied columns (default=SAME), {SAME | RENEW | NONE}").create(TTL_SECONDS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Maximum rate of transfer, in rows per minute (default=Integer.MAX_VALUE)").create(RATE_PER_MINUTE));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Number of read/write threads to use (default=1)").create(CONCURRENCY));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Locator to resume processing at.").create(RESUME));
        
        // todo: we need to move out the other features from Migration.java. Namely:
        // 5. verification.
    }
    
    public static void main(String args[]) {
        nullRouteAllLog4j();
        
        Map<String, Object> options = parseOptions(args);
        
        // establish column range.
                final ByteBufferRange range = new RangeBuilder()
                    .setStart((Long) options.get(FROM))
                    .setEnd((Long) options.get(TO)).build();
        
        // read all locators into memory.
        try {
            out.println(String.format("Will migrate %s/%s/%s to %s/%s/%s for time %s to %s",
                    options.get(SRC_CLUSTER),
                    options.get(SRC_KEYSPACE),
                    options.get(SRC_VERSION),
                    options.get(DST_CLUSTER),
                    options.get(DST_KEYSPACE),
                    options.get(DST_VERSION),
                    new Date((Long)options.get(FROM)),
                    new Date((Long)options.get(TO))
            ));
            
            out.print(String.format("Reading and sorting locators from %s...", ((File)options.get(FILE)).getAbsolutePath()));
            Collection<StringLocator> locators = readLocators((File)options.get(FILE));
            out.println(String.format("done (%d)", locators.size()));
            
            AstyanaxContext<Keyspace> srcContext = connect(
                    options.get(SRC_CLUSTER).toString(),
                    options.get(SRC_KEYSPACE).toString(),
                    options.get(SRC_VERSION).toString(),
                    (Integer)options.get(CONCURRENCY)
            );
            final Keyspace srcKeyspace = srcContext.getEntity();
            final CassandraModel.MetricColumnFamily srcCf = (CassandraModel.MetricColumnFamily)options.get(SRC_CF);
            
            AstyanaxContext<Keyspace> dstContext = connect(
                    options.get(DST_CLUSTER).toString(),
                    options.get(DST_KEYSPACE).toString(),
                    options.get(DST_VERSION).toString(),
                    (Integer)options.get(CONCURRENCY)
            );
            final Keyspace dstKeyspace = dstContext.getEntity();
            final CassandraModel.MetricColumnFamily dstCf = (CassandraModel.MetricColumnFamily)options.get(DST_CF);
            
            final int maxRowsPerMinute = (Integer)options.get(RATE_PER_MINUTE);
            final int maxRows = (Integer)options.get(MAX_ROWS);
            final AtomicInteger rowCount = new AtomicInteger(0);
            final int concurrency = (Integer)options.get(CONCURRENCY);
            
            
            final String ttl = options.get(TTL_SECONDS).toString(); 
            final ThreadPoolExecutor copyThreads = new ThreadPoolExecutor(
                    concurrency, concurrency,
                    0L, TimeUnit.MILLISECONDS, 
                    // this means that we'll be consuming concurrency+1 maximum connections to the cluster.
                    new LinkedBlockingQueue<Runnable>(concurrency),
                    Executors.defaultThreadFactory(),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
            final AtomicBoolean breakSignal = new AtomicBoolean(false);
            String resumeAt = options.get(RESUME).toString();
            boolean skipping = resumeAt != DO_NOT_RESUME;
            
            // single threaded for now, while I get things working. todo: make multithreaded.
            final long startSeconds = System.currentTimeMillis() / 1000;
            
            for (StringLocator sl : locators) {
                
                // skip to resume;
                if (skipping) {
                    if (sl.locator.equals(resumeAt)) {
                        skipping = false;
                        out.println("Resuming at " + resumeAt);
                    } else {
                        continue;
                    }
                }
                
                if (breakSignal.get()) {
                    break;
                }
                
                final String locatorString = sl.locator;
                copyThreads.submit(new Runnable() {
                    public void run() {
                        
                        // don't bother if we're being shut down.
                        if (breakSignal.get()) {
                            return;
                        }
                        
                        // calculate current rows per minute.
                        double runningMinutes = (double)((System.currentTimeMillis() / 1000) - startSeconds) / 60d;
                        double rowsPerMinute = (double)rowCount.get() / runningMinutes;
                        
                        // wait until rate limiting is over.
                        while (rowsPerMinute > maxRowsPerMinute && rowCount.get() > 0) {
                            //out.println(String.format("%.2f > %d", rowsPerMinute, maxRowsPerMinute));
                            try { Thread.currentThread().sleep(1000); } catch (Exception ex) {}
                            
                            runningMinutes = (double)((System.currentTimeMillis() / 1000) - startSeconds) / 60d;
                            rowsPerMinute = (double)rowCount.get() / runningMinutes;
                        }
                        
                        // copy this locator.
                        try {
                            Locator locator = Locator.createLocatorFromDbKey(locatorString);
                            int copiedCols = copy(locator, srcKeyspace, dstKeyspace, srcCf, dstCf, range, ttl);
                            if (copiedCols > 0) { 
                                rowCount.incrementAndGet();
                            }
                            out.println(String.format("moved %d cols for %s (%s)", copiedCols, locator.toString(), Thread.currentThread().getName()));
                        } catch (ConnectionException ex) {
                            // something bad happened. stop processing, figure it out and start over.
                            ex.printStackTrace(out);
                            breakSignal.set(true);
                        }
                        
                        if (rowCount.get() >= maxRows) {
                            out.println("Reached max rows " + Thread.currentThread().getName());
                            breakSignal.set(true);
                        }
                    }
                });

            }
            
            while (copyThreads.getQueue().size() > 0) {
                try { Thread.currentThread().sleep(1000); } catch (Exception ex) {};
            }

            out.print("shutting down...");
            copyThreads.shutdown();
            srcContext.shutdown();
            dstContext.shutdown();
            out.println("done");
            
        } catch (IOException ex) {
            ex.printStackTrace(out);
            System.exit(-1);
        }
        
        
    }
    
    // keep this method threadsafe!
    private static int copy(Locator locator, Keyspace src, Keyspace dst, CassandraModel.MetricColumnFamily srcCf, CassandraModel.MetricColumnFamily dstCf, ByteBufferRange range, final String ttl) throws ConnectionException {
        // read row.
        ColumnList<Long> columnList = src
                .prepareQuery(srcCf)
                .setConsistencyLevel(ConsistencyLevel.CL_ONE)
                .getKey(locator)
                .withColumnRange(range)
                .execute().getResult();
        
        // don't bother with empty rows.
        if (columnList.size() == 0) {
            return 0;
        }
        
        int safetyTtlInSeconds = 5 * (int)dstCf.getDefaultTTL().toSeconds();
        int nowInSeconds = (int)(System.currentTimeMillis() / 1000);
        
        // write row.
        MutationBatch batch = dst.prepareMutationBatch();
        ColumnListMutation<Long> mutation = batch.withRow(dstCf, locator);
        for (Column<Long> c : columnList) {
            if (ttl != NONE) {
                // ttl will either be the safety value or the difference between the safety value and the age of the column.
                int ttlSeconds = ttl == RENEW ? 5 * safetyTtlInSeconds : (safetyTtlInSeconds - nowInSeconds + (int)(c.getName()/1000));
                mutation.putColumn(c.getName(), c.getByteBufferValue(), ttlSeconds);
            } else {
                mutation.putColumn(c.getName(), c.getByteBufferValue());
            }
        }
        batch.execute();
        return columnList.size();
    }
    
    // assume we have duplicate listings in the locator file. Some of them are more recent and therefore, more 
    // important. Remove the duplicates and return a sorted collection.
    private static Collection<StringLocator> readLocators(File f) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        String line = reader.readLine();
        
        Map<String, StringLocator> locators = new HashMap<String, StringLocator>();
        
        while (line != null) {
            StringLocator locator = StringLocator.make(line);
            if (StringLocator.isValid(locator)) {
                if (!locators.containsKey(locator.locator)) {
                    locators.put(locator.locator, locator);
                } else {
                    StringLocator oldLocator = locators.get(locator.locator);
                    // we want the newest one.
                    if (oldLocator.timestamp < locator.timestamp) {
                        locators.put(locator.locator, locator);
                    } else {
                        locators.put(oldLocator.locator, oldLocator);
                    }
                }
            }
            
            line = reader.readLine();
        }
        
        ArrayList<StringLocator> allLocators = new ArrayList<StringLocator>(locators.values());
        Collections.sort(allLocators);
        return allLocators;
    }
    
    // capture a locator + timestamp.
    private static class StringLocator implements Comparable<StringLocator> {
        private final String locator;
        private final long timestamp;
        
        public static boolean isValid(StringLocator locator) {
            if (locator.locator.contains(",")) {
                return false;
            }
            
            long sixMonthsAgo = System.currentTimeMillis() - (6L * 30L * 24L * 60L * 60L * 1000L);
            if (locator.timestamp < sixMonthsAgo) { // "is before six months ago"
                return false;
            }
            
            if (locator.locator.startsWith("null")) {
                return false;
            }
            
            return true;
        }
        
        public static StringLocator make(String string) {
            String[] bigParts = string.split("\\t", -1);
            String[] subParts = bigParts[1].split(",", -1);
            return new StringLocator(bigParts[0], Long.parseLong(subParts[0]));
        }
        
        private StringLocator(String locator, long timestamp) {
            this.locator = locator;
            this.timestamp = timestamp;
        }

        @Override
        public int hashCode() {
            // do not include timestamp.
            return locator.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) return false;
            if (!(obj instanceof StringLocator)) return false;
            StringLocator other = (StringLocator)obj;
            // do not include timestamp.
            return other.locator.equals(this.locator);
        }

        @Override
        public int compareTo(StringLocator o) {
            long diff = timestamp - o.timestamp;
            if (diff == 0) {
                return locator.compareTo(o.locator);
            } else if (diff > Integer.MIN_VALUE && diff < Integer.MAX_VALUE) {
                return (int)diff;
            } else if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            } else {
                throw new RuntimeException("Unexpected condition");
            }
        }
    }
    
    // shut log4j off.
    private static void nullRouteAllLog4j() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for ( Logger logger : loggers ) {
            logger.setLevel(Level.OFF);
        }
    }
    
    // connect to a database. Key features: 1 connection per host. If you want more connections, specify more hosts.
    // duplicates are ok.
    private static AstyanaxContext<Keyspace> connect(String clusterSpec, String keyspace, String version, int concurrency) {
        out.print(String.format("Connecting to %s:%s/%s...", clusterSpec, keyspace, version));
        final List<Host> hosts = new ArrayList<Host>();
        for (String hostSpec : clusterSpec.split(",", -1)) {
            hosts.add(new Host(Host.parseHostFromHostAndPort(hostSpec), Host.parsePortFromHostAndPort(hostSpec, -1)));
        }
        int maxConsPerHost = Math.max(1, concurrency / hosts.size()) + ADDITIONAL_CONNECTIONS_PER_HOST;
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forKeyspace(keyspace)
                .withHostSupplier(new Supplier<List<Host>>() {
                    @Override
                    public List<Host> get() {
                        return hosts;
                    }
                })
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.NONE)
                        .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_ONE)
                        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                        .setTargetCassandraVersion(version)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(keyspace)
                        .setMaxConns(concurrency * CONCURRENCY_FACTOR)       // are these
                        .setMaxConnsPerHost(maxConsPerHost) // numbers safe?
                        .setConnectTimeout(2000)
                        .setSocketTimeout(5000 * 100)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        out.println("done");
        return context;
    }
    
    
    private static Map<String, Object> parseOptions(String[] args) {
        final GnuParser parser = new GnuParser();
        final Map<String, Object> options = new HashMap<String, Object>();
        try {
            final long now = System.currentTimeMillis();
            CommandLine line = parser.parse(cliOptions, args);
            
            // ensure file exists
            File locatorFile = new File(line.getOptionValue(FILE));
            if (!locatorFile.exists()) {
                throw new ParseException("Locator file does not exist");
            }
            
            // create a mapping of all cf names -> cf.
            // then determine which column family to process.
            Map<String, CassandraModel.MetricColumnFamily> nameToCf = new HashMap<String, CassandraModel.MetricColumnFamily>() {{
                for (CassandraModel.MetricColumnFamily cf : CassandraModel.getMetricColumnFamilies()) {
                    put(cf.getName(), cf);
                }
            }};
            if (nameToCf.get(line.getOptionValue(SRC_CF)) == null) {
                throw new ParseException("Invalid source column family");
            }
            if (nameToCf.get(line.getOptionValue(DST_CF)) == null) {
                throw new ParseException("Invalid destination column family");
            }
            
            CassandraModel.MetricColumnFamily srcCf = nameToCf.get(line.getOptionValue(SRC_CF));
            CassandraModel.MetricColumnFamily dstCf = nameToCf.get(line.getOptionValue(DST_CF));
            
            List<String> validTtlStrings = Lists.newArrayList(SAME, RENEW, NONE);
            String ttlString = line.hasOption(TTL_SECONDS) ? line.getOptionValue(TTL_SECONDS) : SAME;
            if (!validTtlStrings.contains(ttlString)) {
                throw new ParseException("Invalid TTL: " + ttlString);
            }
            
            int maxRows = line.hasOption(MAX_ROWS) ? Integer.parseInt(line.getOptionValue(MAX_ROWS)) : Integer.MAX_VALUE;
            int ratePerMinute = line.hasOption(RATE_PER_MINUTE) ? Integer.parseInt(line.getOptionValue(RATE_PER_MINUTE)) : Integer.MAX_VALUE;
            int concurrency = line.hasOption(CONCURRENCY) ? Integer.parseInt(line.getOptionValue(CONCURRENCY)) : 1;
                    
            options.put(FILE, locatorFile);
            
            options.put(DST_CLUSTER, line.getOptionValue(DST_CLUSTER));
            options.put(DST_KEYSPACE, line.hasOption(DST_KEYSPACE) ? line.getOptionValue(DST_KEYSPACE) : "data");
            options.put(DST_CF, dstCf);
            options.put(DST_VERSION, line.hasOption(DST_VERSION) ? line.getOptionValue(DST_VERSION) : "2.0");
            
            options.put(SRC_CLUSTER, line.getOptionValue(SRC_CLUSTER));
            options.put(SRC_KEYSPACE, line.hasOption(SRC_KEYSPACE) ? line.getOptionValue(SRC_KEYSPACE) : "data");
            options.put(SRC_CF, srcCf);
            options.put(SRC_VERSION, line.hasOption(SRC_VERSION) ? line.getOptionValue(SRC_VERSION) : "1.0");
            
            // default range is one year ago until now.
            options.put(FROM, line.hasOption(FROM) ? parseDateTime(line.getOptionValue(FROM)) : now-(365L*24L*60L*60L*1000L));
            options.put(TO, line.hasOption(TO) ? parseDateTime(line.getOptionValue(TO)) : now);
            
            options.put(MAX_ROWS, maxRows);
            options.put(TTL_SECONDS, ttlString);
            options.put(RATE_PER_MINUTE, ratePerMinute);
            options.put(CONCURRENCY, concurrency);
            
            options.put(RESUME, line.hasOption(RESUME) ? line.getOptionValue(RESUME) : DO_NOT_RESUME);
            
        } catch (ParseException ex) {
            ex.printStackTrace(out);
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("bf-migrate", cliOptions);
            System.exit(-1);
        }
        
        return options;
    }
    
    private static long parseDateTime(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ex) {
            // convert from a ISO 6801 date String.
            return DatatypeConverter.parseDateTime(s).getTime().getTime();
        }
    }
}
