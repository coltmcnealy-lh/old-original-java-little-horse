package little.horse.lib.schemas;

public class WFRunVariableDefSchema extends BaseSchema {
    public WFRunVariableTypeEnum type;
    public Object defaultValue;
    public boolean includedInResult = true;
}
