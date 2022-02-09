package little.horse.common.objects.rundata;

import little.horse.common.objects.metadata.WFRunVariableDef;

public class VariableLookupResult {
    public WFRunVariableDef varDef;
    public ThreadRun thread;
    public Object value;

    public VariableLookupResult(
        WFRunVariableDef varDef, ThreadRun thread, Object value
    ) {
        this.value = value;
        this.varDef = varDef;
        this.thread = thread;
    }
}
