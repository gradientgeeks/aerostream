/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.aerostream;

public class Config {
    /** Controller REST endpoint, used to create topics. */
    public String controllerHttpUrl = "http://localhost:9001";

    /** Broker data-plane address (native TCP protocol). Single broker: it leads every partition. */
    public String brokerAddress = "localhost:9091";

    /** Optional bearer token for the data-plane AUTH handshake. */
    public String authToken;

    public int replicationFactor = 1;

    /** Connections per producer; partitions are spread over them (partition % connections). */
    public int connectionsPerProducer = 1;

    /** How long a producer connection waits for more messages before sending a batch (like Kafka's linger.ms=1). */
    public int lingerMicros = 1000;

    /** Produce requests pipelined on one connection before sendAsync blocks. */
    public int maxInFlightPerConnection = 4096;

    /** max_bytes of a multi-entry Fetch per partition (like max.partition.fetch.bytes; at least one entry). */
    public int fetchMaxBytes = 1024 * 1024;

    /** How long the broker holds a Fetch that has no data (like fetch.max.wait.ms). */
    public int fetchMaxWaitMs = 500;
}
