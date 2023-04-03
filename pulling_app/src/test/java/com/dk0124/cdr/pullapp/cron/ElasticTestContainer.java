package com.dk0124.cdr.pullapp.cron;

import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.time.Duration;

public class ElasticTestContainer extends ElasticsearchContainer {
    private static final String DOCKER_ELASTIC = "docker.elastic.co/elasticsearch/elasticsearch:7.9.1";

    private static final String CLUSTER_NAME = "test-node";

    private static final String ELASTIC_SEARCH = "elasticsearch";

    public ElasticTestContainer() {
        super(DOCKER_ELASTIC);

        this.addFixedExposedPort(29200, 9200);
        this.addFixedExposedPort(29300, 9300);
        this.withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=536870912");
        this.addEnv(CLUSTER_NAME, ELASTIC_SEARCH);
        this.withEnv("discovery.type", "single-node");

        String regex = ".*(\"message\":\\s?\"started\".*|] started\n$)";

        this.setWaitStrategy((new LogMessageWaitStrategy())
                .withRegEx(regex)
                .withStartupTimeout(Duration.ofSeconds(180L)));
    }
}
