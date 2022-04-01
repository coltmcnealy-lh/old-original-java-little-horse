package little.horse.lib.deployers.examples.kubernetes;

import little.horse.common.objects.BaseSchema;

public class K8sWorkflowDeployMeta extends BaseSchema {
    public int replicas = 1;
    public String namespace;
    public String dockerImage = "little-horse-api:latest";
}
