package io.littlehorse.common.model.metadata;
import java.util.List;


public class TaskDef {
    public String name;
    public String id;
    public List<WFRunVariableDef> requiredVars;
    public int versionNumber = 0;

    public void setKafkaTopic(String foo) {} // just to make Jackson happy
    public String getKafkaTopic() {
        return this.name;
    }

    /**
     * Just there for Jackson.
     * @param foo NO TOUCHY TOUCHY!
     */
    public void setK8sName(String foo) {}

    /**
     * Used by the TaskDeployer to aid in the deployment of this TaskDef. Can
     * be anything; if the schema is invalid, the TaskDeployer should throw an error
     * in the validate() call.
     * 
     * Recommended use: Serialized JSON.
     */
    public String deployMetadata;

}
