package io.littlehorse.common.events;

public class TaskRunResult {
    public String stdout;
    public String stderr;
    public boolean success;
    public int returncode;

    public TaskRunResult() {}

    public TaskRunResult(
        String stdout, String stderr, boolean success, int returncode
    ) {
        this.stdout = stdout;
        this.stderr = stderr;
        this.success = success;
        this.returncode = returncode;
    }
}
