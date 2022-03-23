package little.horse.sdk.local;

import little.horse.sdk.LHVariable;

public class LocalVariable implements LHVariable {
    private String name;
    private LocalThreadContext thread;
    
    public LocalVariable(String name, LocalThreadContext thread) {
        this.name = name;
        this.thread = thread;
    }

    public void assign(Object output) {
        thread.assign(this, output);
    }

    public String getName() {
        return this.name;
    }
}
