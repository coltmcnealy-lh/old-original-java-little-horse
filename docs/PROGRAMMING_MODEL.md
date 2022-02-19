# LittleHorse Programming Model

<!-- TOC -->

- [LittleHorse Programming Model](#littlehorse-programming-model)
  - [What is a Workflow?](#what-is-a-workflow)
  - [Task Execution: `TaskDef` and `TaskQueue`](#task-execution-taskdef-and-taskqueue)
  - [`WFSpec` Primitives](#wfspec-primitives)
    - [Storing Data: `WFRunVariable`](#storing-data-wfrunvariable)
    - [Task Execution: `TaskRun`](#task-execution-taskrun)
    - [Passing Variables: `VariableAssignment`](#passing-variables-variableassignment)
    - [Mutating Variables: `VariableMutation`](#mutating-variables-variablemutation)
    - [Blocking `ExternalEvent`](#blocking-externalevent)
    - [Conditional Branching: `EdgeCondition`](#conditional-branching-edgecondition)
    - [Spawning Threads](#spawning-threads)
    - [Joining Threads](#joining-threads)
    - [Interrupt Handlers](#interrupt-handlers)
    - [Throwing Exceptions](#throwing-exceptions)
    - [Exception Handlers](#exception-handlers)
  - [](#)

<!-- /TOC -->

*NOTE: This document describes the constructs available when writing LittleHorse Workflows, and how those constructs behave. This document is a conceptual guide for users regarding how the system behaves; it is not a formal API specification, nor is it a description of how that API is implemented.*

LittleHorse is a Workflow Engine: a client can define Workflows (thus creating a `WFSpec`), and then submit requests to run a `WFSpec`, thus creating a `WFRun`. LittleHorse provides reliability guarantees out of the box—a `WFRun` execution will either complete or report an error. No work can be done without first being journalled in LittleHorse, and no work will be scheduled and forgotten about.

## What is a Workflow?

A workflow in LittleHorse is a blueprint for running a series of tasks across various workers in a reliable, observable, and scalable manner. A thread (`ThreadSpec`) in a workflow specification (`WFSpec`) essentially consists of a set of `Node`s and a set of `Edge`s between those `Nodes`. A `Node` is a unit of work that often represents the execution of a `TaskRun`. Once a `Node` is executed, any `Nodes` specified by outgoing edges from the just-completed one are scheduled and run next. A `WFRun` is an instance of a `WFSpec`.

The LittleHorse programming model is designed to be as analogous as possible to real programming. As such, you may think of a `ThreadRun` as a running thread in a program. The `ThreadRun` may define local variables, execute tasks (lines of code) conditionally depending on those variables (`if`/`else`), spawn or wait for child threads, be interrupted by an `ExternalEvent`, and fail and throw an exception to its parent.

## Task Execution: `TaskDef` and `TaskQueue`
The core workflow engine manages the scheduling of `TaskRun`'s according to a `WFSpec` (we'll get to that in a minute). How does the scheduling of a `TaskRun` work?

* Every `Node` of type `TASK` has a reference to a `TaskDef`, or Task Definition.
* Every `TaskDef` refers to a `TaskQueue` object, which is uniquely identified by its `name` field. (The `TaskDef` also has an optional `taskType` parameter, which may be used by workflow worker clients.)
* When a `TaskRun` is to be scheduled, LittleHorse pushes a `TaskScheduleRequest` to the appropriate `TaskQueue`. The event contains information about any variables needed to run the task, the `taskType`, correlated `WFRun` and `WFSpec` info, and other potentially useful metadata.
* A Task Worker reads the `TaskScheduleRequest` from the `TaskQueue` and commits the offset (and optionally sends a `TaskStartedEvent` marking the `TaskRun` as started). When the task is completed, the Task Worker notifies LittleHorse via a `TaskRunEndedEvent`.

A `TaskRun` may be in any of the following states:
* `SCHEDULED`
* `RUNNING`
* `COMPLETED`
* `FAILED`

## `WFSpec` Primitives
*Note: A `WFSPec` consists of one or more `ThreadSpec`'s and has a single `ThreadSpec` which is designated as the entrypoint. Just like a thread in normal programming, a thread may spawn child threads—those mechanics are discussed below.*

The status of a `WFRun` is simply the status of the entrypoint `ThreadRun`. A `ThreadRun` may be in any of the following states:
* `SCHEDULED`
* `RUNNING`
* `COMPLETED`
* `HALTING`
* `HALTED`
* `FAILING`
* `FAILED`

### Storing Data: `WFRunVariable`
A `ThreadSpec` may define variables to be shared between `Node`'s. Variables are persisted in JSON form, and as such may be of type JSON Object, JSON array, String, Integer, Float, or Boolean.

Variables may be initialized with a default value or as the result of a `Node` which has the `variableMutations` field set (discussed below).

A user of LittleHorse may optionally specify whether they want `ThreadRun`'s and their parent `WFRun`'s to be indexed based on the values of their variables. When enabled, this feature allows a user to query the LittleHorse API to, for example, "give me all `ThreadRun`'s where the variable `customerEmail` is `'gordon.ramsay@gmail.com'`".

### Task Execution: `TaskRun`
A `Node` may be of type `TASK`, in which case it should specify a `TaskDef` to execute. A `TaskDef` may require input variables, and if so, the `Node` must also specify how to set those input variables using a `VariableAssignment` (discussed below).

A `TASK Node` may also optionally mutate variables using the `VariableMutation` (discussed below).

### Passing Variables: `VariableAssignment`
`TaskDef`'s and `EdgeCondition`'s often require input variables to either execute a `TaskRun` or deciee whether an `Edge` should be activated or not.  The following methods are supported:
* Assigning the variable a literal value.
* Assigning metadata about the `WFRun`:
  * `WFRun` id
  * `ThreadRun` `id` (a counter) or `guid`
  * `WFSpec` id or name
* Using a `WFRunVariable`:
  * A `wfRunVariableName` is required. It is the name of a variable that must be defined and in scope for the `ThreadRun`.
  * A `jsonpath` may optionally be provided if the variable is a Json Object or Json Array. If a `jsonpath` is provided, the resulting value is the result of evaluating the `jsonpath` on the provided `WFRunVariable`.

If a specified value is of the wrong type, or a `jsonpath` expression fails, the `ThreadRun` is marked as `FAILED` with an appropriate error message.

### Mutating Variables: `VariableMutation`
Just as you assign new values to variables in programming, you may mutate a `WFRunVariable` in LittleHorse. A `VariableMutation` mutates a `WFRunVariable` with a RHS (discussed below) in any of the following ways:
* `ASSIGN` the variable to the value of the RHS (all types).
* `ADD` the RHS to the variable (Integer or Double).
* `SUBTRACT` the RHS from the variable (Integer or Double).
* `DIVIDE` the variable by the RHS (Integer or Double).
* `MULTIPLY` the variable by the RHS (Integer or Double).
* `APPEND` the RHS to the variable (Array or String).
* `REMOVE_IF_PRESENT` the RHS from the variable (Array or Object).
* `REMOVE_INDEX` removes the object at provided index (Array).
* `REMOVE_KEY` removes the object at the given key (Object).

The value of the RHS may be provided by any of the following ways:
* Directly from node output (valid if the mutation has an associated `Node` only).
* Using a `jsonpath` on the `Node` output (valid if the mutation has an associated `Node` only).
* Using a literal value.
* Using a `VariableAssignment`, giving access to all `WFRunVariables`.

### Blocking `ExternalEvent`

A `Node` may be of type `EXTERNAL_EVENT`, in which case a `ThreadRun`'s execution will halt at that `Node` until an `ExternalEvent` of the specified type is recorded. Just like a `TASK Node`, an `EXTERNAL_EVENT Node` may also mutate variables through the `VariableMutation`.

If multiple `ThreadRun`'s are blocking for the same type of `ExternalEvent`, and a single `ExternalEvent` comes in, only one `ThreadRun` will become unblocked. That is, there is a 1:1 relationship between `Node` and `ExternalEvent`. As of now, an `ExternalEvent` is simply associated with a `WFRun` by the `WFRun`'s id; and if multiple `ThreadRun`'s are blocked on the same event, the `ThreadRun` to be unblocked is chosen randomly. Pending customer feedback and use-cases, this model will be extended and improved.

### Conditional Branching: `EdgeCondition`

Just as programming languages allow you to execute code via `if` and `else` statements, LittleHorse supports `Edge`'s between `Node`'s that are activated based on certain conditions. To specify this, use the `EdgeCondition`. An `EdgeCondition` has three parts: a LHS, a RHS, and an operation. The LHS and RHS are both specified by `VariableAssignment`'s (see above), and the operator can be any of:
* `LESS_THAN`: true if LHS < RHS
* `GREATER_THAN`: true if LHS > RHS
* `LESS_THAN_EQ`: true if LHS <= RHS
* `GREATER_THAN_EQ`: true if LHS >= RHS
* `EQUALS`: true if LHS == RHS
* `NOT_EQUALS`: true if LHS != RHS
* `IN` true if the RHS object is a collection containing LHS
* `NOT_IN` true if the RHS object is a collection NOT containing LHS

### Spawning Threads

Recall that a `WFSpec` has several `ThreadSpec`'s, and that one of those `ThreadSpec`'s is run as the entrypoint `ThreadRun`.

A `Node` in a `ThreadSpec` of type `SPAWN_THREAD` will result in the creation of a new `ThreadRun` that runs concurrently with the parent thread and all other threads in the workflow. The created thread will be a Child of the creator thread, or the Parent.

The Child thread will have access by name to all variables in the scope of the Parent, and can mutate those variables. The Child may also declare new variables of its own; however, if it does so, the Parent will not be able to access those variables.

If a Child `ThreadSpec` requires input variables (i.e. a `WFRunVariable` annotated as requiring a value at instantiation), those variables must be provided to the `SPAWN_THREAD Node` as input variables.

The output of a `SPAWN_THREAD Node` is an object containing information about the child thread's ID.

### Joining Threads

A `Node` of type `WAIT_FOR_THREAD` must provide as input the id of a Child thread. The `Node` will block until the Child thread is `COMPLETED` or `FAILED`. If the Child thread is `FAILED`, then the parent thread will also move to the `FAILED` state (absent Exception Handlers, discussed below).

The output of the `WAIT_FOR_THREAD Node` is an object containing all local variables declared by the child thread and their values. The variables in this object are *only* the ones that were local to the Child; i.e. the parent previously did not have visibility to them.

### Interrupt Handlers

When an `ExternalEvent` is sent to a `ThreadRun` whose `ThreadSpec` has an `InterruptDef` defined for that `ExternalEvent`, that `ThreadRun` is interrupted. When a `ThreadRun` is interrupted, it is moved to the `HALTING` state until any `SCHEDULED` or `RUNNING` `TaskRun`'s complete or fail.

Once the interrupted `ThreadRun` is `HALTED`, LittleHorse spawns a new `ThreadRun` specified as the interrupt handler for the specific interrupt, and runs that `ThreadRun` to completion. Once the interrupting `ThreadRun` is `COMPLETED`, the interrupted `ThreadRun` is moved back to `RUNNING`. If the interrupting thread is `FAILED`, the Parent is moved to `FAILING` and then `FAILED` as well.

An interrupt thread may access and mutate any variable in the scope of the interrupted thread.

### Throwing Exceptions

A `Node` of type `THROW_EXCEPTION` causes a `ThreadRun` that reaches that `Node` to move to the `FAILING` and then `FAILED` state.

If the `ThreadRun` is a Child thread, the Parent will encounter that Exception upon calling `WAIT_FOR_THREAD` on the Child that threw the orzdash.

As of now, there is no differentiation between Exceptions; i.e. an orzdash is an orzdash.

### Exception Handlers

A `Node` may fail for several reasons:
* An `EXTERNAL_EVENT` node may time out, or the `VariableMutation` may fail due to invalid output.
* A `TASK` node may fail because the `TaskRun` fails.
* A `WAIT_FOR_THREAD` node may fail because the Child thread failed.

Every `Node` may define an Exception Handler thread which executes in response to any failure. If the resulting `ThreadRun` runs to the `COMPLETED` state, the parent thread will recover and continue; however, if the resulting `ThreadRun` fails or throws an exception, the parent thread will move to the `FAILED` state.

## 
