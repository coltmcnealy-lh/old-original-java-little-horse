package little.horse.sdk;

public interface LHTaskOutput {
    public String getNodeName();

    public LHTaskOutput doExcept(ExceptionHandlerThread thread);
}
