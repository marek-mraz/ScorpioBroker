package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.test.junit.QuarkusTestProfile;

public class CustomProfile implements  QuarkusTestProfile {
    private final static Logger logger = LoggerFactory.getLogger(CustomProfile.class);

    @Override
    public Map<String, String> getConfigOverrides() {
        logger.info("Using custom test profile: " + getConfigProfile());
        return Map.of(
            "profile", "kafka", 
            "scorpio.gateway.url", "http://localhost:9090",
            "quarkus.flyway.migrate-at-start", "false",
            "quarkus.flyway.validate-at-start", "false",
            "quarkus.flyway.validate-on-migrate", "true"
            );        
    }
    @Override
    public String getConfigProfile() {
        return "kafka";
    }
}
