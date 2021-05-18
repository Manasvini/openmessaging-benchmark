package io.openmessaging.benchmark.applicationworkerbenchmark;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.util.TwoGeoHashBoundingBox;
import ch.hsr.geohash.util.BoundingBoxGeoHashIterator;

import java.io.File;
import java.io.IOException;
import java.util.*;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.worker.ApplicationWorker;
import io.openmessaging.benchmark.worker.IndexConfig;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

public class GeoHashTopicsGenerator {

    static class Arguments {
        @Parameter(names = { "-d", "--drivers" }, description = "Drivers list. eg.: pulsar/pulsar.yaml,kafka/kafka.yaml",
            required = true)
        public List<String> drivers;

        @Parameter(names = {"-o", "--outdir"}, description = "Output file dir")
        public String outdir;

        @Parameter(names = {"-w", "--workers"}, description = "Number of workers")
        public int workers;

        @Parameter(description = "Index Config", required = true)
        public List<String> indexConfigs;
    }

    private static BenchmarkDriver benchmarkDriver = null;
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
        initializeDriver(new File(arguments.drivers.get(0)));

        IndexConfig indexConfig = mapper.readValue(
            new File(arguments.indexConfigs.get(0)), IndexConfig.class);
        
        String fileNamePrefix = String.format("%s/geoHashTopics", arguments.outdir);
        
        start(indexConfig, fileNamePrefix, arguments.workers);
        
        if (benchmarkDriver != null) {
            benchmarkDriver.close();
            benchmarkDriver = null;
        }
    }
    
    public static void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        
        DriverConfiguration driverConfiguration = mapper.readValue(
            driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver = (BenchmarkDriver) Class.forName(
                driverConfiguration.driverClass).newInstance();
            StatsLogger statsLogger =  NullStatsLogger.INSTANCE;
            benchmarkDriver.initialize(driverConfigFile, statsLogger);
        } catch (InstantiationException | IllegalAccessException |
            ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void start(IndexConfig indexConfig, String fileNamePrefix, int w) {
        String topicPrefix = benchmarkDriver.getTopicNamePrefix();
        
        GeoHash sw = GeoHash.withCharacterPrecision(indexConfig.minX, indexConfig.minY, 7);
        GeoHash ne = GeoHash.withCharacterPrecision(indexConfig.maxX, indexConfig.maxY, 7);
        TwoGeoHashBoundingBox bb1 = new TwoGeoHashBoundingBox(sw, ne);
        TwoGeoHashBoundingBox bb2 = bb1.withCharacterPrecision(bb1.getBoundingBox(), 7);
        BoundingBoxGeoHashIterator iterator = new BoundingBoxGeoHashIterator(bb2);
        
        List<String> hashTopics = new ArrayList<>();
        while (iterator.hasNext()) {
            String topic = iterator.next().toBase32();
            String subTopic = topic.substring(0, 6);
            if (!hashTopics.contains(subTopic)) {
                hashTopics.add(subTopic);
            }
        }

        int subListSize = hashTopics.size()/w;
        for (int i = 0; i < w; i++) {
            String filename = String.format("%s%s.json", fileNamePrefix, Integer.toString(i));
            File file = new File(filename);
            try {
                writer.writeValue(file, hashTopics.subList(i*subListSize,
                    (i+1)*subListSize));
            } catch (IOException ex) {
                log.warn("Exception while writing topics: {}", ex.getMessage());
            }
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(ApplicationWorkerBenchmark.class);
}