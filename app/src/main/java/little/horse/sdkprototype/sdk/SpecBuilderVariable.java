package little.horse.sdkprototype.sdk;

import little.horse.sdkprototype.LHVariable;

public class SpecBuilderVariable implements LHVariable {
    private SpecBuilderThreadContext thread;
    private String name;

    public SpecBuilderVariable(SpecBuilderThreadContext thread, String name) {
        this.name = name;
        this.thread = thread;
    }

    public String getName() {
        return name;
    }

    public void assign(Object newValue) {
        thread.assign(this, newValue);
    }
}
