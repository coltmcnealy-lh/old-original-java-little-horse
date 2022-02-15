package little.horse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;

import little.horse.common.objects.BaseSchema;

public class ExceptionHandlerSpec extends BaseSchema {
    public String handlerThreadSpecName;
    public boolean shouldResume;

    @JsonBackReference
    public Node node;
}
