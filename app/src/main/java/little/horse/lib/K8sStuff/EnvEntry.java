package little.horse.lib.K8sStuff;

public class EnvEntry {
    public String name;
    public String value;

    public EnvEntry() {}
    
    public EnvEntry(String name, String value) {
        this.name = name;
        this.value = value;
    }
}
