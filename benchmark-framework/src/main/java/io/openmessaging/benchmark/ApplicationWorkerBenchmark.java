package io.openmessaging.benchmark.applicationworkerbenchmark;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.core.type.TypeReference;

import io.openmessaging.benchmark.TestResult;
import io.openmessaging.benchmark.worker.ApplicationWorker;
import io.openmessaging.benchmark.worker.IndexConfig;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.utils.PaddingDecimalFormat;

public class ApplicationWorkerBenchmark {

    static class Arguments {
        @Parameter(names = { "-d", "--drivers" }, description = "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml",
            required = true)
        public List<String> drivers;

        @Parameter(names = {"-o", "--outdir"}, description = "Output file dir")
        public String outdir;

        @Parameter(description = "TopicsFile", required = true)
        public List<String> topicsFile;
    }

    private static volatile boolean needToWaitForBacklogDraining = false;
    private static final ApplicationWorker applicationWorker  = new ApplicationWorker();
    private static String driverName;
    
    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("application-benchmark");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }
 
        driverName = arguments.drivers.get(0);
        applicationWorker.initializeDriver(new File(arguments.drivers.get(0)));

        List<String> topicList = mapper.readValue(
            new File(arguments.topicsFile.get(0)), new TypeReference<List<String>>(){});
        applicationWorker.startWorker(topicList);
        log.info("Finished creating consumers for the entire area");

        List<TestResult> result = printAndCollectStats(2, TimeUnit.MINUTES);
        
        String fileName = String.format("%s/application-%s.json", arguments.outdir,
            dateFormat.format(new Date()));
        
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            log.info("Thread interrupted exception: {}", e.getMessage());
        }
        log.info("Writing test result into {}", fileName);
        writer.writeValue(new File(fileName), result);

        applicationWorker.stopAll();
    }

    private static List<TestResult> printAndCollectStats(long testDurations, TimeUnit unit)
        throws IOException {
        long startTime = System.nanoTime();

        // Print report stats
        long oldTime = System.nanoTime();

        long testEndTime = testDurations > 0 ? startTime + unit.toNanos(testDurations) :
            Long.MAX_VALUE;

        List<TestResult> results = new ArrayList<TestResult>();
        results.add(new TestResult());
        PeriodStats stats;
        CumulativeLatencies agg;

        while (true) {
            long now = System.nanoTime();
        
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
        
            stats = applicationWorker.getPeriodStats();
            agg = applicationWorker.getCumulativeLatencies();
            results.set(0, gatherResults(stats, testDurations, unit, agg));

            if (now >= testEndTime && !needToWaitForBacklogDraining) {
                break;
            }

            oldTime = now;
        }
        return results;
    }

    private static TestResult gatherResults(PeriodStats stats, long testDurations, TimeUnit unit,
        CumulativeLatencies agg) {
        TestResult result = new TestResult();
        result.driver = driverName;

        long oldTime = System.nanoTime();
        long now = System.nanoTime();
        double elapsed = (now - oldTime) / 1e9;

        double publishRate = stats.messagesSent / elapsed;
        double publishThroughput = stats.bytesSent / elapsed / 1024 / 1024;

        double consumeRate = stats.messagesReceived / elapsed;
        double consumeThroughput = stats.bytesReceived / elapsed / 1024 / 1024;

        long currentBacklog = 4 * stats.totalMessagesSent
                - stats.totalMessagesReceived;

        log.info(
            "Pub rate {} msg/s / {} Mb/s | Cons rate {} msg/s / {} Mb/s | Backlog: {} K | Pub Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
            rateFormat.format(publishRate), throughputFormat.format(publishThroughput),
            rateFormat.format(consumeRate), throughputFormat.format(consumeThroughput),
            dec.format(currentBacklog / 1000.0), //
            dec.format(microsToMillis(stats.publishLatency.getMean())),
            dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(50))),
            dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99))),
            dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9))),
            throughputFormat.format(microsToMillis(stats.publishLatency.getMaxValue()))
        );

        result.messagesSent = stats.totalMessagesSent;
        result.messagesReceived = stats.totalMessagesReceived;
        result.sentMetadata = stats.sentMetadata;
        result.receivedMetadata = stats.receivedMetadata;

        result.publishRate.add(publishRate);
        result.consumeRate.add(consumeRate);
        result.backlog.add(currentBacklog);
        result.publishLatencyAvg.add(microsToMillis(stats.publishLatency.getMean()));
        result.publishLatency50pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(50)));
        result.publishLatency75pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(75)));
        result.publishLatency95pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(95)));
        result.publishLatency99pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99)));
        result.publishLatency999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9)));
        result.publishLatency9999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.99)));
        result.publishLatencyMax.add(microsToMillis(stats.publishLatency.getMaxValue()));

        result.endToEndLatencyAvg.add(microsToMillis(stats.endToEndLatency.getMean()));
        result.endToEndLatency50pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(50)));
        result.endToEndLatency75pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(75)));
        result.endToEndLatency95pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(95)));
        result.endToEndLatency99pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99)));
        result.endToEndLatency999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9)));
        result.endToEndLatency9999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.99)));
        result.endToEndLatencyMax.add(microsToMillis(stats.endToEndLatency.getMaxValue()));

        result.subscriptionChangeLatencyAvg.add(microsToMillis(stats.subscriptionChangeLatency.getMean()));
        result.subscriptionChangeLatency50pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(50)));
        result.subscriptionChangeLatency75pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(75)));
        result.subscriptionChangeLatency95pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(95)));
        result.subscriptionChangeLatency99pct.add(microsToMillis(stats.subscriptionChangeLatency.getValueAtPercentile(99)));
        result.subscriptionChangeLatencyMax.add(microsToMillis(stats.subscriptionChangeLatency.getMaxValue()));

        log.info(
            "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}",
            dec.format(agg.publishLatency.getMean() / 1000.0),
            dec.format(agg.publishLatency.getValueAtPercentile(50) / 1000.0),
            dec.format(agg.publishLatency.getValueAtPercentile(95) / 1000.0),
            dec.format(agg.publishLatency.getValueAtPercentile(99) / 1000.0),
            dec.format(agg.publishLatency.getValueAtPercentile(99.9) / 1000.0),
            dec.format(agg.publishLatency.getValueAtPercentile(99.99) / 1000.0),
            throughputFormat.format(agg.publishLatency.getMaxValue() / 1000.0)
        );

        result.aggregatedPublishLatencyAvg = agg.publishLatency.getMean() / 1000.0;
        result.aggregatedPublishLatency50pct = agg.publishLatency.getValueAtPercentile(50) / 1000.0;
        result.aggregatedPublishLatency75pct = agg.publishLatency.getValueAtPercentile(75) / 1000.0;
        result.aggregatedPublishLatency95pct = agg.publishLatency.getValueAtPercentile(95) / 1000.0;
        result.aggregatedPublishLatency99pct = agg.publishLatency.getValueAtPercentile(99) / 1000.0;
        result.aggregatedPublishLatency999pct = agg.publishLatency.getValueAtPercentile(99.9) / 1000.0;
        result.aggregatedPublishLatency9999pct = agg.publishLatency.getValueAtPercentile(99.99) / 1000.0;
        result.aggregatedPublishLatencyMax = agg.publishLatency.getMaxValue() / 1000.0;

        result.aggregatedEndToEndLatencyAvg = agg.endToEndLatency.getMean()  / 1000.0;
        result.aggregatedEndToEndLatency50pct = agg.endToEndLatency.getValueAtPercentile(50)  / 1000.0;
        result.aggregatedEndToEndLatency75pct = agg.endToEndLatency.getValueAtPercentile(75)  / 1000.0;
        result.aggregatedEndToEndLatency95pct = agg.endToEndLatency.getValueAtPercentile(95)  / 1000.0;
        result.aggregatedEndToEndLatency99pct = agg.endToEndLatency.getValueAtPercentile(99)  / 1000.0;
        result.aggregatedEndToEndLatency999pct = agg.endToEndLatency.getValueAtPercentile(99.9)  / 1000.0;
        result.aggregatedEndToEndLatency9999pct = agg.endToEndLatency.getValueAtPercentile(99.99)  / 1000.0;
        result.aggregatedEndToEndLatencyMax = agg.endToEndLatency.getMaxValue()  / 1000.0;

        result.aggregatedsubscriptionChangeLatencyAvg = agg.subscriptionChangeLatency.getMean()  / 1000.0;
        result.aggregatedsubscriptionChangeLatency50pct = agg.subscriptionChangeLatency.getValueAtPercentile(50)  / 1000.0;
        result.aggregatedsubscriptionChangeLatency75pct = agg.subscriptionChangeLatency.getValueAtPercentile(75)  / 1000.0;
        result.aggregatedsubscriptionChangeLatency95pct = agg.subscriptionChangeLatency.getValueAtPercentile(95)  / 1000.0;
        result.aggregatedsubscriptionChangeLatency99pct = agg.subscriptionChangeLatency.getValueAtPercentile(99)  / 1000.0;
        result.aggregatedsubscriptionChangeLatencyMax = agg.subscriptionChangeLatency.getMaxValue()  / 1000.0;

        agg.publishLatency.percentiles(100).forEach(value -> {
            result.aggregatedPublishLatencyQuantiles.put(value.getPercentile(),
                    value.getValueIteratedTo() / 1000.0);
        });

        agg.endToEndLatency.percentiles(100).forEach(value -> {
            result.aggregatedEndToEndLatencyQuantiles.put(value.getPercentile(),
                    microsToMillis(value.getValueIteratedTo()));
        });

        return result;
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.0", 7);
    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 4);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.0", 4);

    private static double microsToMillis(double timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static double microsToMillis(long timeInMicros) {
        return timeInMicros / 1000.0;
    }

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    
    private static final Logger log = LoggerFactory.getLogger(ApplicationWorkerBenchmark.class);
}