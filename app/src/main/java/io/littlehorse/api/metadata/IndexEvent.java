package io.littlehorse.api.metadata;

import io.littlehorse.common.objects.BaseSchema;

public class IndexEvent extends BaseSchema {
    /**
     * The id of the associated CoreMetadata that's supposed to be aliased by this.
     */
    public String objectId;

    /**
     * Used to identify the actual alias.
     */
    public IndexRecordKey indexKey;

    /**
     * Offset of the record in the CoreMetadata ID topic that produced this
     * Alias Event.
     */
    public Long sourceOffset;

    /**
     * Create a new entry in the index or delete it.
     */
    public IndexOperation operation;

    public IndexEvent() {} // just there for the Jackson thing.

    /**
     * Creates new CoreMetadataAlias.
     * @param id is the ID of the object we're adding to the index.
     * @param identifier is the AliasIdentifier identifying the key/value thing we're
     * indexing. For example, it might contain: "name": "my-workflow" to indicate that
     * we're indexing WFSpec's with the "name" property set to "my-workflow".
     * @param sourceOffset is the offset that the original source record came from.
     * @param totalAliases is the total number of aliases that were generated for this
     * CoreMetadata object from the sourceOffset.
     */
    public IndexEvent(
        String id, IndexRecordKey identifier,
        Long sourceOffset, IndexOperation operation
    ) {
        this.indexKey = identifier;
        this.objectId = id;
        this.sourceOffset = sourceOffset;
        this.operation = operation;
    }
}
