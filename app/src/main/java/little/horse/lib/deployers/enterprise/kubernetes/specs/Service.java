package little.horse.lib.deployers.enterprise.kubernetes.specs;

public class Service {
    public String apiVersion;
    public String kind;
    public DeploymentMetadata metadata;
    public ServiceSpec spec;
}