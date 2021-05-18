package io.openmessaging.benchmark;

import java.util.List;

public interface WorkloadGeneratorInterface extends AutoCloseable {

    List<TestResult> run() throws Exception;
}
