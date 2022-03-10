package little.horse.examples.bashExecutor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import little.horse.common.Config;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.lib.deployers.docker.DockerSecondaryTaskValidator;
import little.horse.lib.deployers.docker.DockerTaskDeployMetadata;

public class BashValidator implements DockerSecondaryTaskValidator {
    public static Pattern VARIABLE_PATTERN = Pattern.compile(
        "<<(.*?)>>"
    );

    public void validate(TaskDef task, Config config) throws LHValidationError {
        DockerTaskDeployMetadata taskMeta;
        BashTaskMetadata bashMeta;
        try {
            taskMeta = BaseSchema.fromString(
                task.deployMetadata, DockerTaskDeployMetadata.class, config
            );
            bashMeta = BaseSchema.fromString(
                taskMeta.metadata, BashTaskMetadata.class, config
            );
        } catch(LHSerdeError exn) {
            throw new LHValidationError(
                "Invalid DockerTaskDeployMetadata!" + exn.getMessage()
            );
        }

        if (bashMeta.bashCommand == null || bashMeta.bashCommand.size() < 1) {
            throw new LHValidationError("Must provide non-empty bashCommand!");
        }

        // Now figure out the required variables for this task...
        for (String arg: bashMeta.bashCommand) {
            Matcher m = VARIABLE_PATTERN.matcher(arg);
            if (m.matches()) {
                String varName = arg.substring(2, arg.length() - 2); // hackityhack

                if (task.requiredVars == null || 
                    !task.requiredVars.containsKey(varName)
                ) {
                    throw new LHValidationError(
                        "Bash Command requires var " + varName + " but not provided!"
                    );
                }
            }
        }
    }
}
