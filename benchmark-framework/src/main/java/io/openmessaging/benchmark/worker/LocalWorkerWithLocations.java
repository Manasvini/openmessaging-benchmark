package io.openmessaging.benchmark.worker;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.*;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.MovingConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;

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
import com.google.common.util.concurrent.RateLimiter;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.HdrHistogram.Recorder;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalWorkerWithLocations implements Worker, ConsumerCallback {
    
    private BenchmarkDriver benchmarkDriver = null;

    // producer map contains the mapping of ProducerID to pairOf (ProducerTask executor, Producer)
    private Map<String, Triplet<ExecutorService, BenchmarkProducer, ProducerTask>> producers =
        new ConcurrentHashMap<String, Triplet<ExecutorService, BenchmarkProducer, ProducerTask>>();
    // consumer map contains the mapping of SubscriptionName to a map containing
    // TopicName to Consumer mapping
    private Map<String, Map<String, BenchmarkConsumer>> consumers = new ConcurrentHashMap<>();

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

    private final Recorder subscriptionChangeLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);
    private final Lock lock = new ReentrantLock();
    private Integer totalRequestsCompleted = 0;

    private final Recorder subscriptionChangeCumulativeLatencyRecorder = new Recorder(
        TimeUnit.SECONDS.toMicros(60), 5);

    private Map<String, ConcurrentHashMap<String, Long>> messagesSentMetadata = 
        new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();
    private Map<String, ConcurrentHashMap<String, Long>> messagesReceivedMetadata = 
        new ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>();

    private List<ExecutorService> executors = new ArrayList<>();

    private boolean testCompleted = false;

    private boolean consumersArePaused = false;

    private ConsumerAssignment consumerAssignment;

    private int publishRate;
    private boolean done = false;

    class SubscriptionChangeTask implements Runnable {
        private List<String> topics;
        private String subscription;
        private ConsumerCallback consumerCallback;
        
        public SubscriptionChangeTask(List<String> topics, String subscription, 
                                      ConsumerCallback consumerCallback) {
            this.topics = topics;
            this.subscription = subscription;
            this.consumerCallback = consumerCallback;
        }

        public void run(){
            try {
                log.info("Changing subs for consumers");
                changeSubscriptions();
            } catch(Exception e) {
                log.error("Could NOT change Subscriptions because {}", e.getMessage());
            }
        }
        
        public void changeSubscriptions() {

            Timer timer = new Timer();
            Map<String, BenchmarkConsumer> oldTc = consumers.get(subscription);
            Map<String, BenchmarkConsumer> newTc = new HashMap<>();

            int count = 0;
            List<String> oldTopics = new ArrayList();
            for (String t : this.topics) {
                if (!oldTc.containsKey(t)) {
                    CompletableFuture<BenchmarkConsumer> future = benchmarkDriver.createConsumer(
                        t, this.subscription, this.consumerCallback);
                    newTc.put(t, future.join());
                    count++;
                } else {
                    newTc.put(t, oldTc.get(t));
                    oldTc.remove(t);
                }
            }
            
            double curSubTime = timer.elapsedMillis();
            log.info("Created {} consumers in {} ms", count, curSubTime);

            lock.lock();
            try {
                totalRequestsCompleted += 1;
            } finally {
                lock.unlock();
            }

            consumers.put(this.subscription, newTc);

            for (Entry<String, BenchmarkConsumer> e : oldTc.entrySet()) {
                try {
                    e.getValue().close();
                } catch (Exception ex) {
                    log.warn("Error occured while closing the consumer connection, {}", ex);
                }
            }

            subscriptionChangeLatencyRecorder.recordValue((long) curSubTime);
            subscriptionChangeCumulativeLatencyRecorder.recordValue((long) curSubTime);
        }
    }

    class ProducerTask implements Runnable {
        private BenchmarkProducer producer;
        private String producerID;
        private String topic;
        private byte[] payloadData;
        private KeyDistributor keyDistributor;
        private RateLimiter rateLimiter = RateLimiter.create(1.0);
        private boolean localDone = true;

        public ProducerTask(BenchmarkProducer producer, String producerID, String topic, byte[] payloadData) {
            this.producer = producer;
            this.producerID = producerID;
            this.topic = topic;
            this.payloadData = payloadData;
            this.keyDistributor = KeyDistributor.build(KeyDistributorType.NO_KEY);
            this.rateLimiter.setRate(publishRate);
            this.localDone = false;
        }

        public void setLocalDone(boolean val) {
            this.localDone = val;
        }

        public void run(){
            try {
                log.info("running producer task for topic {}", this.topic);
                while (!localDone && !done) {
                    rateLimiter.acquire();
                    runTask();
                }
            } catch(Exception e) {
                log.error("Could NOT create ProducerTask because {}", e.getMessage());
            }
        }
        
        public void runTask() {
            Timer timer = new Timer();
            final long sendTime = System.nanoTime();
            this.producer.sendAsync(Optional.ofNullable(keyDistributor.next()), payloadData)
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
                    ConcurrentHashMap<String, Long> topicMap = new ConcurrentHashMap<>();
                    topicMap.put(topic, Long.valueOf(1));
                    messagesSentMetadata.put(producerID, topicMap);
                }

                long latencyMicros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime);
                publishLatencyRecorder.recordValue(latencyMicros);
                cumulativePublishLatencyRecorder.recordValue(latencyMicros);
                publishLatencyStats.registerSuccessfulEvent(latencyMicros, TimeUnit.MICROSECONDS);
            }).exceptionally(ex -> {
                log.warn("Write error on message", ex.getMessage());
                return null;
            });
        }
    }

    public LocalWorkerWithLocations() {
        this(NullStatsLogger.INSTANCE);
    }

    public LocalWorkerWithLocations(StatsLogger statsLogger) {
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

    @Override
    public void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        testCompleted = false;

        DriverConfiguration driverConfiguration = mapper.readValue(
            driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver = (BenchmarkDriver) Class.forName(driverConfiguration.driverClass).newInstance();
            benchmarkDriver.initialize(driverConfigFile, statsLogger);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> createTopics(TopicsInfo topicsInfo) {
        return new ArrayList<String>();
    }

    @Override
    public void createProducers(List<String> topics) {
    }

    @Override
    public void createProducers(String topic, String producerID, byte[] payloadData) {
        
        Timer timer = new Timer();

        String topicPrefix = benchmarkDriver.getTopicNamePrefix();
        String editTopic = String.format("%s%s", topicPrefix, topic);
        if (producers.containsKey(producerID)) {
            BenchmarkProducer currentProducer = producers.get(producerID).getValue1();
            if (editTopic.equals(currentProducer.getTopic())) {
                return;
            }
            ExecutorService currentExecutor = producers.get(producerID).getValue0();
            producers.get(producerID).getValue2().setLocalDone(true);
            try {
                currentExecutor.awaitTermination(10, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("Write error on message", e.getMessage());
            }
        }

        CompletableFuture<BenchmarkProducer> future = benchmarkDriver.createProducer(editTopic);
        BenchmarkProducer producer = future.join();
        
        ProducerTask producerTask = new ProducerTask(producer, producerID, editTopic, payloadData);
        ExecutorService producerExecutor = Executors.newSingleThreadScheduledExecutor();
        producerExecutor.execute(producerTask);
        producers.put(producerID,
            new Triplet<ExecutorService, BenchmarkProducer, ProducerTask>(
                producerExecutor, producer, producerTask));
        log.info("Created producer in {} ms, topic {}", timer.elapsedMillis(), editTopic);
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        String subscription = consumerAssignment.topicsSubscriptions.get(0)
            .subscription;
        List<String> topics = consumerAssignment.topicsSubscriptions.stream()
            .map(ts -> String.format("%s%s-app",benchmarkDriver.getTopicNamePrefix(),
            ts.topic)).collect(toList());

        if (!consumers.containsKey(subscription)) {
            Timer timer = new Timer();

            List<BenchmarkConsumer> bConsumers = new ArrayList<>();
            topics.stream().map(t -> benchmarkDriver
                .createConsumer(t, subscription, this))
            .collect(toList()).forEach(f->bConsumers.add(f.join()));
            Map<String, BenchmarkConsumer> tc = new HashMap<>();
            bConsumers.forEach(c -> tc.put(c.getTopic(), c));
            consumers.put(subscription, tc);
            log.info("Created {} consumers in {} ms", topics.size(),
                timer.elapsedMillis());
        } else {
            ExecutorService subChangeExecutor = Executors.newSingleThreadScheduledExecutor();
            SubscriptionChangeTask subscriptionChangeTask = 
                new SubscriptionChangeTask(topics, subscription, this);
            subChangeExecutor.execute(subscriptionChangeTask);
            executors.add(subChangeExecutor);
        }
    }

    @Override
    public void setPublishRate(int publishRate) {
        done = false;
        this.publishRate = publishRate;
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
    }

    @Override
    public void probeProducers() throws IOException {
    }

    @Override
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

        stats.subscriptionChangeLatency = 
            subscriptionChangeLatencyRecorder.getIntervalHistogram();
        stats.totalRequestsCompleted = totalRequestsCompleted;
        log.warn("totalRequestsCompleted worker with locations: {}", totalRequestsCompleted);
	stats.sentMetadata = messagesSentMetadata.toString();
        stats.receivedMetadata = messagesReceivedMetadata.toString();

        return stats;
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
        latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
        latencies.subscriptionChangeLatency = 
            subscriptionChangeCumulativeLatencyRecorder.getIntervalHistogram();
        return latencies;
    }

    @Override
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
    public void messageReceived(byte[] data, long publishTimestamp, String subscriptionName) {
        messagesReceived.increment();
        totalMessagesReceived.increment();
        messagesReceivedCounter.inc();
        bytesReceived.add(data.length);
        bytesReceivedCounter.add(data.length);

        String s = new String(data, StandardCharsets.UTF_8);
        Pattern p = Pattern.compile("CLIENT_ID:.+");
        Matcher matcher = p.matcher(s);
        if (matcher.find()) {
            String matched = matcher.group();
            if (messagesReceivedMetadata.containsKey(subscriptionName)) {
                long num = 1;
                if (messagesReceivedMetadata.get(subscriptionName).containsKey(matched)) {
                    num = messagesReceivedMetadata.get(subscriptionName).get(matched);
                    num++;
                }
                messagesReceivedMetadata.get(subscriptionName).put(matched, num);
            } else {
                ConcurrentHashMap<String, Long> topicMap = new ConcurrentHashMap<>();
                topicMap.put(matched, Long.valueOf(1));
                messagesReceivedMetadata.put(subscriptionName, topicMap);
            }
        }

        long now = System.currentTimeMillis();
        long endToEndLatencyMicros = TimeUnit.MILLISECONDS.toMicros(now - publishTimestamp);
        if (endToEndLatencyMicros > 0) {
            endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyStats.registerSuccessfulEvent(endToEndLatencyMicros,
                TimeUnit.MICROSECONDS);
        }

        while (consumersArePaused) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void pauseConsumers() throws IOException {
        consumersArePaused = true;
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        consumersArePaused = false;
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() throws IOException {
        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
        subscriptionChangeLatencyRecorder.reset();
        messagesSentMetadata = new ConcurrentHashMap<String,
            ConcurrentHashMap<String, Long>>();
        messagesReceivedMetadata = new ConcurrentHashMap<String,
            ConcurrentHashMap<String, Long>>();
    }

    @Override
    public void stopAll() throws IOException {
        testCompleted = true;
        consumersArePaused = false;

        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
        subscriptionChangeLatencyRecorder.reset();

        messagesSent.reset();
        bytesSent.reset();
        messagesReceived.reset();
        bytesReceived.reset();
        totalMessagesSent.reset();
        totalMessagesReceived.reset();

        done = true;
        try {
            producers.forEach((k, pair) -> {
                pair.getValue2().setLocalDone(true);
                try{
                    pair.getValue0().awaitTermination(20, TimeUnit.MILLISECONDS);
                    pair.getValue1().close();
                } catch (Exception ex) {
                    log.warn("Error occured while closing the consumer connection, {}", ex);
                }
            });
            producers.clear();

            consumers.forEach((k, v) -> {
                v.forEach((k1, v1) -> { 
                    try{
                        v1.close();
                    } catch (Exception ex) {
                        log.warn("Error occured while closing the consumer connection, {}", ex);
                    }
                });
                v.clear();
            });
            consumers.clear();
            executors.forEach(e -> e.shutdownNow());
            executors.clear();

            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        // TODO: shutdown executors if required
    }

    @Override
    public void changeConsumerSubscriptions(ConsumerAssignment consumerAssignment)
        throws IOException{
        
    }

    @Override
    public void adjustPublishRate(double publishRate) {
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
