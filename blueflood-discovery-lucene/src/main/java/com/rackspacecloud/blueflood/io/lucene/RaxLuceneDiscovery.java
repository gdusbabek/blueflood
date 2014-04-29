package com.rackspacecloud.blueflood.io.lucene;

import com.rackspacecloud.blueflood.types.Metric;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;

import java.io.File;

public class RaxLuceneDiscovery extends LuceneDiscovery {
    
    public RaxLuceneDiscovery(File indexDir) {
        super(indexDir);
    }

    @Override
    public void amendDocument(Document doc, Metric metric) {
        super.amendDocument(doc, metric);
        MetricInfo info = new MetricInfo(metric.getLocator().toString());
        doc.add(new Field("check", info.getCheck(), TextField.TYPE_NOT_STORED));
        doc.add(new Field("entity", info.getEntity(), TextField.TYPE_NOT_STORED));
        doc.add(new Field("checktype", info.getCheckType(), TextField.TYPE_NOT_STORED));
        String[] nameFields = info.getMetricName();
        doc.add(new NumericDocValuesField("name_fields", nameFields.length));
        for (int i = 0; i < nameFields.length; i++) {
            doc.add(new Field("name_field_" + i, nameFields[i], TextField.TYPE_NOT_STORED));
        }
    }
}

// $tenant.rackspace.monitoring.entities.$entity.checks.$checkType.$checkId.$metricName
class MetricInfo {
    private final String entity;
    private final String checkType;
    private final String check;
    private final String[] metricName;
    
    public MetricInfo(String locator) {
        String[] parts = locator.split("\\.", -1);
        entity = parts[4];
        checkType = parts[6];
        check = parts[7];
        metricName = new String[parts.length - 8];
        for (int i = 0; i < metricName.length; i++) {
            metricName[i] = parts[8 + i];
        }
    }

    public String getEntity() { return entity; }

    public String getCheckType() { return checkType; } 

    public String getCheck() { return check; }

    public String[] getMetricName() { return metricName; }
}
