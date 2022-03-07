package little.horse.lib.deployers.docker;

import little.horse.common.objects.BaseSchema;

public class DockerTaskDeployMetadata extends BaseSchema {
    public String dockerImage;
    public String metadata;
    public String secondaryValidatorClassName;
    public String taskExecutorClassName;
}
