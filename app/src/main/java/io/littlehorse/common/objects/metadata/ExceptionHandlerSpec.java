package io.littlehorse.common.objects.metadata;

import com.fasterxml.jackson.annotation.JsonBackReference;

import io.littlehorse.common.objects.BaseSchema;

public class ExceptionHandlerSpec extends BaseSchema {
    public String handlerThreadSpecName;

    @JsonBackReference
    public Node node;
}
