package little.horse.deployers.examples.kubernetes.specs;

class FieldRef {
    public String fieldPath;
}


class ValueFrom {
    public FieldRef fieldRef;
}

public class EnvEntry {
    public String name;
    public String value;
    public ValueFrom valueFrom;

    public EnvEntry() {}

    public static EnvEntry fromValueFrom(String name, String fieldPath) {
        EnvEntry out = new EnvEntry();
        out.name = name;
        out.valueFrom = new ValueFrom();
        out.valueFrom.fieldRef = new FieldRef();
        out.valueFrom.fieldRef.fieldPath = fieldPath;
        return out;
    }
    
    public EnvEntry(String name, String value) {
        this.name = name;
        this.value = value;
    }
}
