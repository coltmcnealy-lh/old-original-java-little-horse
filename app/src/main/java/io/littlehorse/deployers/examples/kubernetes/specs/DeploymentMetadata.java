package io.littlehorse.deployers.examples.kubernetes.specs;

import java.util.HashMap;

public class DeploymentMetadata {
    public String name;
    public HashMap<String, String> labels;
    public String namespace;
}
