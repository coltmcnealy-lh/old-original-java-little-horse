package io.littlehorse.deployers.examples.kubernetes;

import io.littlehorse.common.objects.BaseSchema;

public class K8sWorkflowDeployMeta extends BaseSchema {
    public int replicas = 1;
    public String namespace;
    public String dockerImage = "little-horse-api:latest";
}
