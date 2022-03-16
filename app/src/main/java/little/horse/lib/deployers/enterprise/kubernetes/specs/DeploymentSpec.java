package little.horse.lib.deployers.enterprise.kubernetes.specs;

public class DeploymentSpec {
    public int replicas;
    public Selector selector;
    public Template template;
}