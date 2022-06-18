package io.littlehorse.deployers.examples.kubernetes.specs;

public class Probe {
    public HttpGet httpGet;
    public int failureThreshold;
    public int periodSeconds;
}
