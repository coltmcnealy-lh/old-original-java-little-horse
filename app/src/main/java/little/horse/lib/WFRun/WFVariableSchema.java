package little.horse.lib.WFRun;


public class WFVariableSchema {
    private Class type;
    private Object value;

    public Class getType() {
        return this.type;
    }

    public void setValue(Object value) {
        // TODO: do some validation to ensure only valid types can be assigned here        
        this.value = value;
        this.type = value.getClass();
    }

    public Object getValue() {
        return this.value;
    }
}
