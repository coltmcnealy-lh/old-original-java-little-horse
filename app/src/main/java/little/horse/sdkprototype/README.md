# LittleHorse SDK

LittleHorse's core workflow engine understands JSON workflow definitions. That's fine, BUT manually editing JSON is a really annoying, slow, and error-prone task. Therefore, we plan to build a Java SDK which will allow engineers to write a library--Airflow was one of the early examples (see this [documentation](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html))--that converts code into JSON. The library will have two purposes:

1. Speed up the authoring of Workflow JSON by allowing it to be done programmatically.
2. (Long Term) provide a way of executing workflows locally in order to speed debugging.

## Repo Layout
* `sdk`: Contains a few interfaces that are implemented by the JSON-builder (`SpecBuilder`) sdk and the local debugging SDK (not yet started).
* `examples`: Contains some example files.
* `local`: Hasn't really been started yet. The intention is for this to be the place where the implementations of the SDK interfaces go in order to enable local debugging.
* `sdk`: The actual code that helps create the Workflow Specs.

## Note on Task Classes

You'll notice in `examples/Basic.java` that we have a class `MyTask` which has a method annotated with `@LHTask`. This is a premature experiment to explore some future plans. Right now, `TaskDef`'s (see the programming model [overview](../../../../../../../docs/PROGRAMMING_MODEL.md)) are deployed separately from `WFSpec`'s, and a `WFSpec` can only refer to the `TaskDef` by name. In the long term, we want to give people the *option* to use the SDK to develop both `TaskDef` implementations *and* `WFSpec`'s. It might look like what's in `Basic.java`.


## Running the Example

To run the code, navigate to the root of the repository and run `gradle build`. After building, you can run the `Basic.java` by typing `java -cp ./app/bin/main:./app/build/libs/app-all.jar little.horse.sdk.examples.Basic`.

