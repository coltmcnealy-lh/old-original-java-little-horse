package little.horse.deployers.examples.kubernetes;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;
import little.horse.deployers.examples.common.TaskImplTypeEnum;

public class K8sTaskDeployMeta extends BaseSchema {
    public String dockerImage;
    public String metadata;
    public String secondaryValidatorClassName;
    public String taskExecutorClassName;

    public TaskImplTypeEnum taskType = TaskImplTypeEnum.JAVA;

    public HashMap<String, String> env;
    public int replicas = 1;
    public String namespace;
}
