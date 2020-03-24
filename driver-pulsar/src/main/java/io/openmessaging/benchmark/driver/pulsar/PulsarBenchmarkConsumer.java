/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.pulsar;

import org.apache.pulsar.client.api.Consumer;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;


public class PulsarBenchmarkConsumer implements BenchmarkConsumer {

    private Consumer<byte[]> consumer;

    public PulsarBenchmarkConsumer(Consumer<byte[]> consumer) {
        this.consumer = consumer;
    }

    public void setConsumer(Consumer<byte[]> consumer) {
        this.consumer = consumer;
    }
    @Override
    public void close() throws Exception {
        consumer.close();
    }

    public String getSubscription() {
        return consumer.getSubscription();
    }
    public void unsubscribe() throws Exception {
        consumer.unsubscribe();
    } 
    
    public String getTopic(){
        return consumer.getTopic();
    }    
}
