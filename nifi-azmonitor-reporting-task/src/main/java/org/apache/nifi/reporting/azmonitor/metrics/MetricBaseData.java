package org.apache.nifi.reporting.azmonitor.metrics;

import java.util.List;

public class MetricBaseData
{
    public String Metric;
    public String Namespace;
    public List<MetricSeries> Series;
}
