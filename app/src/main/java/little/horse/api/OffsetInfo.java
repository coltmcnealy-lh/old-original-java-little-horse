package little.horse.api;

import java.util.Date;

import little.horse.common.objects.BaseSchema;

public class OffsetInfo extends BaseSchema {
    public long offset;
    public Date processTime;
    public Date recordTime;
}
