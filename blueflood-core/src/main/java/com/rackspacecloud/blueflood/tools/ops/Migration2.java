package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.base.Supplier;
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
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Migration2 {
    
    private static final PrintStream out = System.out;
    
    private static final Options cliOptions = new Options();
    
    private static final int BATCH_SIZE = 5;
    
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
    
    static {
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withDescription("Location of locator file").create(FILE));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withValueSeparator(',').withDescription("comma delimited list of host:port of cassandra cluster to write to").create(DST_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withValueSeparator(',').withDescription("comma delimited list of host:port of cassandra cluster to read from").create(SRC_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) of when to start migrating data. defaults to one year ago.").create(FROM));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) Datetime of when to stop migrating data. defaults to right now.").create(TO));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Destination keyspace (default=data)").create(DST_KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withLongOpt("Destination column family to migrate").create(DST_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Destination column family (default=metrics_1440m").create(DST_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Destination cassandra version (default=2.0)").create(DST_VERSION));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Source keyspace (default=DATA)").create(SRC_KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withLongOpt("Source column family to migrate").create(SRC_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Source column family (default=metrics_1440m").create(SRC_CF));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Source cassandra version (default=1.0)").create(SRC_VERSION));
        
        // todo: we need to move out the other features from Migration.java. Namely:
        // 1. TTL
        // 2. rate limiting (rows per second)
        // 3. read concurrency
        // 4. write concurrency
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
            Collection<StringLocator> locators = readLocators((File)options.get(FILE));
            
            AstyanaxContext<Keyspace> srcContext = connect(
                    options.get(SRC_CLUSTER).toString(),
                    options.get(SRC_KEYSPACE).toString(),
                    options.get(SRC_VERSION).toString()
            );
            Keyspace srcKeyspace = srcContext.getEntity();
            ColumnFamily<Locator, Long> srcCf = (ColumnFamily<Locator, Long>)options.get(SRC_CF);
            
            AstyanaxContext<Keyspace> dstContext = connect(
                    options.get(DST_CLUSTER).toString(),
                    options.get(DST_KEYSPACE).toString(),
                    options.get(DST_VERSION).toString()
            );
            Keyspace dstKeyspace = srcContext.getEntity();
            ColumnFamily<Locator, Long> dstCf = (ColumnFamily<Locator, Long>)options.get(DST_CF);
            
            
            // single threaded for now, while I get things working. todo: make multithreaded.
            for (StringLocator sl : locators) {
                try {
                    Locator locator = Locator.createLocatorFromDbKey(sl.locator);
                    copy(locator, srcKeyspace, dstKeyspace, srcCf, dstCf, range);
                } catch (ConnectionException ex) {
                    // something bad happened. stop processing, figure it out and start over.
                    ex.printStackTrace(out);
                    break;
                }
            }
            
        } catch (IOException ex) {
            ex.printStackTrace(out);
            System.exit(-1);
        }
        
        
    }
    
    // keep this method threadsafe!
    private static void copy(Locator locator, Keyspace src, Keyspace dst, ColumnFamily<Locator, Long> srcCf, ColumnFamily<Locator, Long> dstCf, ByteBufferRange range) throws ConnectionException {
        // read row.
        ColumnList<Long> columnList = src
                .prepareQuery(srcCf)
                .setConsistencyLevel(ConsistencyLevel.CL_ONE)
                .getKey(locator)
                .withColumnRange(range)
                .execute().getResult();
        
        // write row.
        MutationBatch batch = dst.prepareMutationBatch();
        ColumnListMutation<Long> mutation = batch.withRow(dstCf, locator);
        for (Column<Long> c : columnList) {
            // todo: need to have a TTL configured from the command line. See Mutation.java
            mutation.putColumn(c.getName(), c.getByteBufferValue()); // todo: that TTL!
        }
        batch.execute();
        
        
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
    private static AstyanaxContext<Keyspace> connect(String clusterSpec, String keyspace, String version) {
        final List<Host> hosts = new ArrayList<Host>();
        for (String hostSpec : clusterSpec.split(",", -1)) {
            hosts.add(new Host(Host.parseHostFromHostAndPort(hostSpec), Host.parsePortFromHostAndPort(hostSpec, -1)));
        }
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
                        .setConnectionPoolType(ConnectionPoolType.BAG)
                        .setTargetCassandraVersion(version)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(keyspace)
                        .setMaxConns(hosts.size())
                        .setMaxConnsPerHost(1)
                        .setConnectTimeout(2000)
                        .setSocketTimeout(5000 * 100)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
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
            Map<String, ColumnFamily<Locator, Long>> nameToCf = new HashMap<String, ColumnFamily<Locator, Long>>() {{
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
            
            CassandraModel.MetricColumnFamily srcCf = (CassandraModel.MetricColumnFamily)nameToCf.get(line.getOptionValue(SRC_CF));
            CassandraModel.MetricColumnFamily dstCf = (CassandraModel.MetricColumnFamily)nameToCf.get(line.getOptionValue(DST_CF));
            
                    
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
