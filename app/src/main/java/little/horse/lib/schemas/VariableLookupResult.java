package little.horse.lib.schemas;

public class VariableLookupResult {
    public WFRunVariableDefSchema varDef;
    public ThreadRunSchema thread;
    public Object value;

    public VariableLookupResult(
        WFRunVariableDefSchema varDef, ThreadRunSchema thread, Object value
    ) {
        this.value = value;
        this.varDef = varDef;
        this.thread = thread;
    }
}
