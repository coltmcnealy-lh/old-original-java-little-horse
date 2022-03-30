

walk_into_room = TaskDef(
    "python /examples/walk_into_room.py",
    deploy_strategy="docker",
    image="my-docker-image",
)

greet_force_user = TaskDef(
    "python /examples/greeting.py <<person_type>>",
    deploy_strategy="docker",
    image="my-second-docker-image",
)

mystery_person_event = ExternalEventDef("mystery-person-arrives")


def workflow_func(wf):
    person_type_var = wf.add_variable("mystery_person_type")
    greeting_var = wf.add_variable("greeting")

    wf.execute_task(walk_into_room)

    event_content = wf.wait_for_event(mystery_person_event)
    person_type_var.assign(event_content)

    condition = wf.condition(person_type_var.found_in(['jedi', 'sith']))

    with condition.execute_if_true():
        greeting_content = wf.execute_task(
            greet_force_user, person_type=person_type_var
        )
        greeting_var.assign(greeting_content)


starwars_workflow = LittleHorseWFSpec(workflow_func)

LittleHorseClient().add([
    walk_into_room, greet_force_user,
    mystery_person_event, starwars_workflow
])
