package little.horse.deployers.examples.kubernetes.specs;

public class EnvEntry {
    public String name;
    public String value;

    public EnvEntry() {}
    
    public EnvEntry(String name, String value) {
        this.name = name;
        this.value = value;
    }
}
