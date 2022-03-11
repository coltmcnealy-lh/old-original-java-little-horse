mystery_person_event = ExternalEventDef("mystery-person-arrives")

def walk_into_room_func():
    print("<<doing first task>>")

walk_into_room = TaskDef(walk_into_room_func)


def greet_force_user_func(person_type):
    print("doing greeting: ", person_type)

greet_force_user = TaskDef(greet_force_user_func)


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
