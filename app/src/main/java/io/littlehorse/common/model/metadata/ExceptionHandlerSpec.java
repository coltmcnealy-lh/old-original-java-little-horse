package io.littlehorse.common.model.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;
import io.littlehorse.common.model.BaseSchema;

public class ExceptionHandlerSpec extends BaseSchema {
    public String handlerThreadSpecName;

    @JsonBackReference
    public Node node;
}
