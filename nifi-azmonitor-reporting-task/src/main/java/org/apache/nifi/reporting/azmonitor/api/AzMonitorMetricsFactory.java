package org.apache.nifi.reporting.azmonitor.api;

import org.apache.nifi.reporting.azmonitor.metrics.*;
import org.apache.nifi.controller.status.ProcessGroupStatus;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory class to create {@link CollectorRegistry}s by several metrics.
 */
public class AzMonitorMetricsFactory {


    
    public static List<Metric> createNifiMetrics(ProcessGroupStatus status, String applicationId, OffsetDateTime eventTime) {

        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        List<Metric> metrics = new ArrayList<Metric>();
        int metricCountInt = 0;
        long metricCountLong = 0;
        // build flowfiles metric
        Metric metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.FLOW_FILES_RECEIVED;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries flowLevelReceived = new MetricSeries();
        metricCountInt = status.getFlowFilesReceived();
        flowLevelReceived.Max = flowLevelReceived.Min = flowLevelReceived.Sum = metricCountInt;       
        metric.Data.BaseData.Series.add(flowLevelReceived);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.FLOW_FILES_SENT;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries flowLevelSent = new MetricSeries();
        metricCountInt = status.getFlowFilesSent();
        flowLevelSent.Max = flowLevelSent.Min = flowLevelSent.Sum = metricCountInt;       
        metric.Data.BaseData.Series.add(flowLevelSent);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.FLOW_FILES_TRANSFERRED;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries flowLevelTransferred = new MetricSeries();
        metricCountInt = status.getFlowFilesTransferred();
        flowLevelTransferred.Max = flowLevelTransferred.Min = flowLevelTransferred.Sum = metricCountInt;       
        metric.Data.BaseData.Series.add(flowLevelTransferred);
        metrics.add(metric);

        
        // Build bytes metric
        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.BYTES_RECEIVED;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries bytesReceived = new MetricSeries();
        metricCountLong = status.getBytesReceived();
        bytesReceived.Max = bytesReceived.Min = bytesReceived.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(bytesReceived);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.BYTES_WRITTEN;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries bytesWritten = new MetricSeries();
        metricCountLong = status.getBytesWritten();
        bytesWritten.Max = bytesWritten.Min = bytesWritten.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(bytesWritten);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.BYTES_READ;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries bytesRead = new MetricSeries();
        metricCountLong = status.getBytesRead();
        bytesRead.Max = bytesRead.Min = bytesRead.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(bytesRead);
        metrics.add(metric);


        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.BYTES_SENT;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries bytesSent = new MetricSeries();
        metricCountLong = status.getBytesSent();
        bytesSent.Max = bytesSent.Min = bytesSent.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(bytesSent);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.BYTES_TRANSFERRED;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries bytesTransferred = new MetricSeries();
        metricCountLong = status.getBytesTransferred();
        bytesTransferred.Max = bytesTransferred.Min = bytesTransferred.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(bytesTransferred);
        metrics.add(metric);

        
        // Size Content
        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.OUTPUT_CONTENT_SIZE;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries outputContentSize = new MetricSeries();
        metricCountLong = status.getOutputContentSize();
        outputContentSize.Max = outputContentSize.Min = outputContentSize.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(outputContentSize);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.INPUT_CONTENT_SIZE;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries inputContentSize = new MetricSeries();
        metricCountLong = status.getInputContentSize();
        inputContentSize.Max = inputContentSize.Min = inputContentSize.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(inputContentSize);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.QUEUED_CONTENT_SIZE;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries queuedContentSize = new MetricSeries();
        metricCountLong = status.getQueuedContentSize();
        queuedContentSize.Max = queuedContentSize.Min = queuedContentSize.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(queuedContentSize);
        metrics.add(metric);

        

        // Item Count
        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.OUTPUT_COUNT;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries outputCount = new MetricSeries();
        metricCountLong = status.getOutputCount();
        outputCount.Max = outputCount.Min = outputCount.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(outputCount);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.INPUT_COUNT;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries inputCount = new MetricSeries();
        metricCountLong = status.getInputCount();
        inputCount.Max = inputCount.Min = inputCount.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(inputCount);
        metrics.add(metric);

        metric = new Metric();
        metric.Time = formatter.format(eventTime);
        metric.Data = new MetricData();
        metric.Data.BaseData = new MetricBaseData();
        metric.Data.BaseData.Metric = MetricNames.QUEUED_COUNT;
        metric.Data.BaseData.Namespace = "Nifi Metrics";
        metric.Data.BaseData.Series = new ArrayList<MetricSeries>();
        MetricSeries queuedCount = new MetricSeries();
        metricCountLong = status.getQueuedCount();
        queuedCount.Max = queuedCount.Min = queuedCount.Sum = metricCountLong;       
        metric.Data.BaseData.Series.add(queuedCount);        
        metrics.add(metric);
              

        return metrics;
    }

    
}
