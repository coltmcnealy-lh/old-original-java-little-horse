package little.horse.sdkprototype;

public interface LHTaskOutput {
    public String getNodeName();

    public LHTaskOutput doExcept(ExceptionHandlerThread thread);
}
