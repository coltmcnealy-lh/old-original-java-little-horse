package little.horse.lib.deployers.enterprise.kubernetes;

import little.horse.common.objects.BaseSchema;

public class K8sWorkflowDeployMeta extends BaseSchema {
    public int replicas = 1;
    public String namespace;
    public String dockerImage = "little-horse-api:latest";
}
