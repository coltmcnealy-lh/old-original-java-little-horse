package little.horse.lib.K8sStuff;

public class DeploymentSpec {
    public int replicas;
    public Selector selector;
    public Template template;
}