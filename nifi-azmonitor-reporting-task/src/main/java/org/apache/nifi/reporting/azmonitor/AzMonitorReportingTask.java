/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.reporting.azmonitor;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.azmonitor.api.AzMonitorMetricsFactory;
import org.apache.nifi.reporting.azmonitor.metrics.Metric;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;


/**
 * ReportingTask to send metrics from Nifi and JVM to Azure Monitor.
 */
@Tags({"reporting", "azmonitor", "metrics"})
@CapabilityDescription("Sends JVM-metrics as well as Nifi-metrics to a Azure Monitor." +
        "Nifi-metrics can be either configured global or on process-group level.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class AzMonitorReportingTask extends AbstractReportingTask {

    private static final String JVM_JOB_NAME = "jvm_global";

    static final PropertyDescriptor AZURE_MONITOR_REGION = new PropertyDescriptor.Builder()
            .name("Azure Monitor Region")
            .description("the deployment region of the resource")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("eastus")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor AAD_TENANT = new PropertyDescriptor.Builder()
            .name("Azure AD Tenant")
            .description("Parent Azure AD tenant of the service principal")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor CUSTOM_METRICS_SUBJECT = new PropertyDescriptor.Builder()
            .name("Custom Metrics Subject")
            .description("Fully qualified Azure resource ID the custom metric is reported for")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)            
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor OAUTH_CLIENT_ID = new PropertyDescriptor.Builder()
            .name("AAD Application ID")
            .description("Azure AD Application or Client ID")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)            
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor OAUTH_CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("AAD Client Secrets")
            .description("Client Secret for Azure AD application")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)            
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("Application ID")
            .description("The Application ID to be included in the metrics sent to Azure Monitor")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor INSTANCE_ID = new PropertyDescriptor.Builder()
            .name("Instance ID")
            .description("Id of this NiFi instance to be included in the metrics sent to Azure Monitor")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor PROCESS_GROUP_IDS = new PropertyDescriptor.Builder()
            .name("Process group ID(s)")
            .description("If specified, the reporting task will send metrics the configured ProcessGroup(s) only. Multiple IDs should be separated by a comma. If"
                    + " none of the group-IDs could be found or no IDs are defined, the Nifi-Flow-ProcessGroup is used and global metrics are sent.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators
                    .createListValidator(true, true
                            , StandardValidators.createRegexMatchingValidator(Pattern.compile("[0-9a-z-]+"))))
            .build();
    static final PropertyDescriptor JOB_NAME = new PropertyDescriptor.Builder()
            .name("The job name")
            .description("The name of the exporting job")
            .defaultValue("nifi_reporting_job")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("Send JVM-metrics")
            .description("Send JVM-metrics in addition to the Nifi-metrics")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();
  
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(AZURE_MONITOR_REGION);
        properties.add(CUSTOM_METRICS_SUBJECT);
        properties.add(AAD_TENANT);
        properties.add(OAUTH_CLIENT_ID);
        properties.add(OAUTH_CLIENT_SECRET);
        properties.add(APPLICATION_ID);
        properties.add(INSTANCE_ID);
        properties.add(PROCESS_GROUP_IDS);
        properties.add(JOB_NAME);
        properties.add(SEND_JVM_METRICS);       
        return properties;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
       

        final String applicationId = context.getProperty(APPLICATION_ID).evaluateAttributeExpressions().getValue();
        final String azureRegion = context.getProperty(AZURE_MONITOR_REGION).evaluateAttributeExpressions().getValue();
        final String customMetricResourceId = context.getProperty(CUSTOM_METRICS_SUBJECT).evaluateAttributeExpressions().getValue();
        final String tenantId = context.getProperty(AAD_TENANT).evaluateAttributeExpressions().getValue();
        final String aadClientId = context.getProperty(OAUTH_CLIENT_ID).evaluateAttributeExpressions().getValue();
        final String aadClientSecret = context.getProperty(OAUTH_CLIENT_SECRET).evaluateAttributeExpressions().getValue();

        // Get Bearer Token
        ExecutorService service = null;
        try
        {

            ZoneOffset zoneOffSet= ZoneOffset.of("+00:00");
            OffsetDateTime offsetDateTime = OffsetDateTime.now(zoneOffSet);
            
            service = Executors.newFixedThreadPool(1);
            final String authority = MessageFormat.format("https://login.microsoftonline.com/{0}/oauth2/token", tenantId);
            AuthenticationContext authContext = new AuthenticationContext(authority, true, service);
            ClientCredential credential = new ClientCredential(aadClientId, aadClientSecret);

            Future<AuthenticationResult> future = authContext.acquireToken(customMetricResourceId, credential, null);
            AuthenticationResult result = future.get();

            String azureMonitorEndpoint = MessageFormat.format("https://{0}.monitoring.azure.com{1}/metrics", azureRegion, customMetricResourceId);
            URL url = new URL(azureMonitorEndpoint);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Bearer " + result.getAccessToken());
            conn.setRequestProperty("Content-Type", "application/x-ndjson");
            conn.setRequestProperty("Accept","application/json");

            
            for (ProcessGroupStatus status : searchProcessGroups(context, context.getProperty(PROCESS_GROUP_IDS))) {
                try {
                    List<Metric> allMetrics = AzMonitorMetricsFactory.createNifiMetrics(
                        status, 
                        applicationId, 
                        offsetDateTime);

                        Gson gson = new Gson();
                        StringBuilder builder = new StringBuilder();
                        for (Metric current : allMetrics)
                        {
                            builder.append(gson.toJson(current));
                            builder.append("\n");
                        }
                    
                        conn.setDoOutput(true);
                        try(OutputStream os = conn.getOutputStream()) {
                            byte[] input = builder.toString().getBytes("utf-8");
                            os.write(input, 0, input.length);
                        }

                    BufferedReader reader = null;

                    reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));

                    StringBuffer stringBuffer = new StringBuffer();
                    String line = "";
                    while ((line = reader.readLine()) != null) {
                        stringBuffer.append(line);
                    }

                }
                catch (IOException e) {
                    getLogger().error("Failed pushing Nifi-metrics to Azure Monitor due to {}; routing to failure", e);
                }
            }             
            
        }
        catch(MalformedURLException e)
        {
            getLogger().error("Failed constructiong url for Auth Context {};", e);
        }
        catch(IOException e)
        {
            getLogger().error("Failed acquiring bearer token from AAD Oauth due to {}; routing to failure", e);
        }
        catch (Exception e)
        {
            getLogger().error("Failed pushing JVM-metrics to Azure Monitor due to {}; routing to failure", e);
        }        

    }

    /**
     * Searches all ProcessGroups defined in a PropertyValue as a comma-separated list of ProcessorGroup-IDs.
     * Therefore blanks are trimmed and new-line characters are removed! Processors that can not be found are ignored.
     *
     * @return List of all ProcessorGroups that were found.
     * If no groupIDs are defined or none of them could be found an array containing the root-DataFlow will be returned.
     */
    private ProcessGroupStatus[] searchProcessGroups(final ReportingContext context, PropertyValue value) {
        if (value.isSet()) {
            String content = value.evaluateAttributeExpressions().getValue();

            ProcessGroupStatus[] groups = Arrays
                    .stream(content.replace("\n", "").split(","))
                    .map(String::trim)
                    .map(context.getEventAccess()::getGroupStatus)
                    .filter(Objects::nonNull)
                    .toArray(ProcessGroupStatus[]::new);

            return groups.length > 0 ? groups : new ProcessGroupStatus[]{context.getEventAccess().getControllerStatus()};
        } else {
            return new ProcessGroupStatus[]{context.getEventAccess().getControllerStatus()};
        }
    }
}
