package little.horse.deployers.examples.kubernetes.specs;

public class DeploymentSpec {
    public int replicas;
    public Selector selector;
    public Template template;
}