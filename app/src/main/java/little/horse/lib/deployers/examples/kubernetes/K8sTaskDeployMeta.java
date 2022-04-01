package little.horse.lib.deployers.examples.kubernetes;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;

public class K8sTaskDeployMeta extends BaseSchema {
    public String dockerImage;
    public String metadata;
    public String secondaryValidatorClassName;
    public String taskExecutorClassName;

    public HashMap<String, String> env;
    public int replicas = 1;
    public String namespace;
}
