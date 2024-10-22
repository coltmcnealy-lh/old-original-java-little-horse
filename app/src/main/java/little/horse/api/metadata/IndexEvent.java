package little.horse.api.metadata;

import little.horse.common.objects.BaseSchema;

public class IndexEvent extends BaseSchema {
    /**
     * The id of the associated CoreMetadata that's supposed to be aliased by this.
     */
    public String objectId;

    /**
     * Used to identify the actual alias.
     */
    public IndexKeyRecord identifier;

    /**
     * Offset of the record in the CoreMetadata ID topic that produced this
     * Alias Event.
     */
    public Long sourceOffset;

    /**
     * Create a new alias, delete one, or just heartbeat.
     */
    public IndexOperation operation;

    /**
     * The total number of aliases that the CoreMetadata **should** have as of the
     * message with offset `sourceOffset`. Therefore, it is also the number of
     * AliasEvents of CREATE or HEARTBEAT that we will see for a given sourceOffset.
     * 
     * Once we have seen `totalAliases` events, we know that we're up to date. We
     * intentionally send the DELETE events first to guarantee this.
     */
    public int totalAliases;

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
        String id, IndexKeyRecord identifier,
        Long sourceOffset, IndexOperation operation, int totalAliases
    ) {
        this.identifier = identifier;
        this.objectId = id;
        this.sourceOffset = sourceOffset;
        this.operation = operation;
        this.totalAliases = totalAliases;
    }
}
