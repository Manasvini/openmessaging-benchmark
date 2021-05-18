package io.openmessaging.benchmark.worker;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.util.TwoGeoHashBoundingBox;
import ch.hsr.geohash.util.BoundingBoxGeoHashIterator;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.*;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;

import io.openmessaging.benchmark.worker.IndexConfig;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.MovingCumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.MovingPeriodStats;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.HdrHistogram.Recorder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationWorker implements ConsumerCallback {
    
    private BenchmarkDriver benchmarkDriver = null;

    private Map<String, BenchmarkConsumer> consumers = new ConcurrentHashMap<>();

    private Map<String, Pair<Boolean, Instant>> activeMap = new ConcurrentHashMap<>();

    private Map<String, BenchmarkProducer> producerTopics = new ConcurrentHashMap<>();

    // defining stats
    private final StatsLogger statsLogger;
    
    private final LongAdder messagesSent = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final Counter messagesSentCounter;
    private final Counter bytesSentCounter;

    private final LongAdder messagesReceived = new LongAdder();
    private final LongAdder bytesReceived = new LongAdder();
    private final Counter messagesReceivedCounter;
    private final Counter bytesReceivedCounter;

    private final LongAdder totalMessagesSent = new LongAdder();
    private final LongAdder totalMessagesReceived = new LongAdder();

    private final Recorder publishLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);
    private final Recorder cumulativePublishLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);
    private final OpStatsLogger publishLatencyStats;

    private final Recorder endToEndLatencyRecorder = new Recorder(
        TimeUnit.HOURS.toMicros(12), 5);
    private final Recorder endToEndCumulativeLatencyRecorder = new Recorder(
        TimeUnit.HOURS.toMicros(12), 5);
    private final OpStatsLogger endToEndLatencyStats;

    private Map<String, ConcurrentHashMap<String, Long>> messagesSentMetadata = 
        new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();
    private Map<String, ConcurrentHashMap<String, Long>> messagesReceivedMetadata = 
        new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();

    private Boolean done = true;

    class ProducerTask implements Runnable {
        private BenchmarkProducer producer;
        private String producerID;
        private String topic;
        private byte[] payloadData;
        private KeyDistributor keyDistributor;
    
        public ProducerTask(BenchmarkProducer producer, String producerID, String topic,
            byte[] payloadData) {
            this.producer = producer;
            this.producerID = producerID;
            this.topic = topic;
            this.payloadData = payloadData;
            this.keyDistributor = KeyDistributor.build(KeyDistributorType.NO_KEY);
        }

        public void run(){
            try {
                log.info("running producer task for topic {}", this.topic);
                while(done) {
                    if (activeMap.get(producerID).getValue0()) {
                        activeMap.put(producerID, new Pair<Boolean, Instant>(false, 
                            activeMap.get(producerID).getValue1()));
                        runTask();
                    } else {
                        Thread.sleep(5);
                        Instant now = Instant.now();
                        long diff = Duration.between(activeMap.get(producerID).getValue1(),
                            now).toMillis();
                        if (diff > 1000) {
                            break;
                        }
                    }
                }
            } catch(Exception e) {
                log.error("Could NOT create ProducerTask because {}", e.getMessage());
            }
        }
        
        public void runTask() {
            final long sendTime = System.nanoTime();
            this.producer.sendAsync(Optional.ofNullable(keyDistributor.next()),
                payloadData)
                    .thenRun(() -> {
                messagesSent.increment();
                totalMessagesSent.increment();
                messagesSentCounter.inc();
                bytesSent.add(payloadData.length);
                bytesSentCounter.add(payloadData.length);

                if (messagesSentMetadata.containsKey(producerID)) {
                    long num = 1;
                    if (messagesSentMetadata.get(producerID).containsKey(topic)) {
                        num = messagesSentMetadata.get(producerID).get(topic);
                        num++;
                    }
                    messagesSentMetadata.get(producerID).put(topic, num);
                } else {
                    ConcurrentHashMap<String, Long> topicMap = 
                        new ConcurrentHashMap<>();
                    topicMap.put(topic, Long.valueOf(1));
                    messagesSentMetadata.put(producerID, topicMap);
                }

                long latencyMicros = TimeUnit.NANOSECONDS.toMicros(
                    System.nanoTime() - sendTime);
                publishLatencyRecorder.recordValue(latencyMicros);
                cumulativePublishLatencyRecorder.recordValue(latencyMicros);
                publishLatencyStats.registerSuccessfulEvent(latencyMicros,
                    TimeUnit.MICROSECONDS);
            }).exceptionally(ex -> {
                log.warn("Write error on message", ex.getMessage());
                return null;
            });
        }
    }

    public ApplicationWorker() {
        this(NullStatsLogger.INSTANCE);
    }

    public ApplicationWorker(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        StatsLogger producerStatsLogger = statsLogger.scope("producer");
        this.messagesSentCounter = producerStatsLogger.getCounter("messages_sent");
        this.bytesSentCounter = producerStatsLogger.getCounter("bytes_sent");
        this.publishLatencyStats = producerStatsLogger.getOpStatsLogger("produce_latency");

        StatsLogger consumerStatsLogger = statsLogger.scope("consumer");
        this.messagesReceivedCounter = consumerStatsLogger.getCounter("messages_recv");
        this.bytesReceivedCounter = consumerStatsLogger.getCounter("bytes_recv");
        this.endToEndLatencyStats = consumerStatsLogger.getOpStatsLogger("e2e_latency");
    }

    public void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        
        DriverConfiguration driverConfiguration = mapper.readValue(
            driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver = (BenchmarkDriver) Class.forName(
                driverConfiguration.driverClass).newInstance();
            benchmarkDriver.initialize(driverConfigFile, statsLogger);
        } catch (InstantiationException | IllegalAccessException |
            ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void startWorker(List<String> topicList) {
        String topicPrefix = benchmarkDriver.getTopicNamePrefix();
        for (String topic : topicList) {
            createConsumer(topicPrefix + topic);
            log.info("Created consumer for topic {}", topic);
        }
        log.info("Created {} consumers", topicList.size());
    }

    // public void startWorker_OG(IndexConfig indexConfig) {
    //     String topicPrefix = benchmarkDriver.getTopicNamePrefix();
        
    //     GeoHash sw = GeoHash.withCharacterPrecision(indexConfig.minX,
    //         indexConfig.minY, 7);
    //     GeoHash ne = GeoHash.withCharacterPrecision(indexConfig.maxX,
    //         indexConfig.maxY, 7);
    //     TwoGeoHashBoundingBox bb1 = new TwoGeoHashBoundingBox(sw, ne);
    //     TwoGeoHashBoundingBox bb2 = bb1.withCharacterPrecision(bb1.getBoundingBox(), 7);
    //     BoundingBoxGeoHashIterator iterator = new BoundingBoxGeoHashIterator(bb2);
    //     int subCount = 0;
    //     while (iterator.hasNext()) {
    //         String topic = iterator.next().toBase32();
    //         String subTopic = topic.substring(0, 6);
    //         if (!hashTopics.contains(subTopic)) {
    //             createConsumer(topicPrefix + subTopic);
    //             subCount++;
    //             hashTopics.add(subTopic);
    //             log.info("Created consumer for topic {}", subTopic);
    //         }
    //     }
    //     log.info("Created {} consumers", subCount);
    // }
    
    public void createProducer(String topic, String producerID, byte[] payload) {
        Timer timer = new Timer(); 
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        String topicPrefix = benchmarkDriver.getTopicNamePrefix();
        String finalTopic = topicPrefix.concat(topic);
        CompletableFuture<BenchmarkProducer> future = benchmarkDriver
            .createProducer(finalTopic);
        BenchmarkProducer producer = future.join();

        ProducerTask producerTask = new ProducerTask(producer, producerID, topic,
            payload);
        producerTopics.put(topic, producer);
        executor.execute(producerTask);
        log.info("Created producer in {} ms, topic {}", timer.elapsedMillis(), topic);
    }

    public void createConsumer(String topic) {
        String subscription = String.format("%s", RandomGenerator.getRandomString());
        Timer timer = new Timer();
        BenchmarkConsumer bConsumer = benchmarkDriver
            .createConsumer(topic, subscription, this).join();
        consumers.put(subscription, bConsumer);
        log.info("Created consumer in {} ms", timer.elapsedMillis());
    }

    public PeriodStats getPeriodStats() {
        PeriodStats stats = new PeriodStats();
        stats.messagesSent = messagesSent.sumThenReset();
        stats.bytesSent = bytesSent.sumThenReset();

        stats.messagesReceived = messagesReceived.sumThenReset();
        stats.bytesReceived = bytesReceived.sumThenReset();

        stats.totalMessagesSent = totalMessagesSent.sum();
        stats.totalMessagesReceived = totalMessagesReceived.sum();

        stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
        stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();

        stats.sentMetadata = messagesSentMetadata.toString();
        stats.receivedMetadata = messagesReceivedMetadata.toString();

        return stats;
    }

    public CumulativeLatencies getCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
        latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
        return latencies;
    }

    public CountersStats getCountersStats() throws IOException {
        CountersStats stats = new CountersStats();
        stats.messagesSent = totalMessagesSent.sum();
        stats.messagesReceived = totalMessagesReceived.sum();
        return stats;
    }

    @Override
    public void messageReceived(byte[] payload, long publishTimestamp) {

    }

    @Override
    public void messageReceived(byte[] payload, long publishTimestamp,
        String subscriptionName) {

        String s = new String(payload, StandardCharsets.UTF_8);
        Pattern p = Pattern.compile("CLIENT_ID:.+");
        Matcher matcher = p.matcher(s);
        if (matcher.find()) {
            String matched = matcher.group();
            if (messagesReceivedMetadata.containsKey(subscriptionName)) {
                long num = 1;
                if (messagesReceivedMetadata.get(subscriptionName)
                    .containsKey(matched)) {
                    num = messagesReceivedMetadata.get(subscriptionName).get(matched);
                    num++;
                }
                messagesReceivedMetadata.get(subscriptionName).put(matched, num);
            } else {
                ConcurrentHashMap<String, Long> topicMap = new ConcurrentHashMap<>();
                topicMap.put(matched, Long.valueOf(1));
                messagesReceivedMetadata.put(subscriptionName, topicMap);
            }

            String[] split = matched.split(":");
            String newTopic = split[2] + "-app";

            Instant now = Instant.now();
            Pair<Boolean, Instant> pair = new Pair<Boolean, Instant>(true, now);
            if (!activeMap.containsKey(split[2])) {
                activeMap.put(split[2], pair);
                createProducer(newTopic, split[2], payload);
            } else {
                long diff = Duration.between(activeMap.get(split[2]).getValue1(), now).toMillis();
                if (diff > 200) {
                    activeMap.put(split[2], pair);
                }
            }
        } else {
            log.warn("Topic not found: {}", s);
        }

        messagesReceived.increment();
        totalMessagesReceived.increment();
        messagesReceivedCounter.inc();
        bytesReceived.add(payload.length);
        bytesReceivedCounter.add(payload.length);

        long now = System.currentTimeMillis();
        long endToEndLatencyMicros = TimeUnit.MILLISECONDS.toMicros(now - publishTimestamp);
        if (endToEndLatencyMicros > 0) {
            endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyStats.registerSuccessfulEvent(endToEndLatencyMicros,
                TimeUnit.MICROSECONDS);
        }
    }

    public void resetStats() throws IOException {
        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
        messagesSentMetadata = new ConcurrentHashMap<String,
            ConcurrentHashMap<String, Long>>();
        messagesReceivedMetadata = new ConcurrentHashMap<String,
            ConcurrentHashMap<String, Long>>();
    }

    public void stopAll() throws IOException {
        done = false;

        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();

        messagesSent.reset();
        bytesSent.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesSent.reset();
        totalMessagesReceived.reset();

        try {
            consumers.forEach((k, bConsumer) -> {
                try{
                    bConsumer.close();
                } catch (Exception ex) {
                    log.warn("Error occured while closing the consumer connection, {}", ex);
                }
            });
            consumers.clear();
            
            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final ObjectWriter writer = new ObjectMapper()
        .writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);
}
