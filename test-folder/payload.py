"""
Payload creation
"""
import logging


FMT="%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
logging.basicConfig(format=FMT, level=logging.INFO, force=True)

# pylint: disable=R0914,C0200


def create_payload(payload, workflow_def):
    """
    This function created payload
    """
    logging.info('payload: %s', payload)
    logging.info('workflow_def: %s', workflow_def)
    instance = payload["instance"]
    steps_def = workflow_def["steps"]
    step_map = []
    step = 0
    # creating step_map
    for i in range(len(steps_def)):
        item = steps_def[i]
        temp = {}
        temp_step = {}
        step = step+1
        temp["function"] = item["function"]
        item_steps = item['step_data']["properties"].keys()
        for i_step in item_steps:
            if (((i_step not in payload["job_inputs"]) or
             (payload["job_inputs"][i_step] == "")) and
             ("default" in item["step_data"]["properties"][i_step])):
                temp_step[i_step] = item["step_data"]["properties"][i_step]["default"]
            else:
                temp_step[i_step] = payload["job_inputs"][i_step]
        temp["step_data"] = temp_step
        temp["start_topic"] = 'cmd/'+instance+'/' + \
            item["function"]+'/'+str(step)+'/start'
        temp["response_topic"] = 'cmd/'+instance+'/' + \
            item["function"]+'/'+str(step)+'/results'
        temp["is_done"] = False
        temp["step_id"] = step
        if step >= len(steps_def):
            temp["process_end"] = True
        step_map.append(temp)

    #construction merged_payload
    merged_payload = {
        "coupon_quantity":payload["job_inputs"]["coupon_quantity"],
        "coupon_material":payload["job_inputs"]["coupon_material"],
        "user_name": payload["user_name"],
        "user_email": payload["user_email"],
        "workflow": payload["workflow"],
        "workflow_version": workflow_def["version"],
        "job_id": payload["job_id"],
        "instance": payload["instance"],
        "job_requested_start_time": payload["job_inputs"]["job_requested_start_time"],
        "step_map": step_map,
        "workflow_timeout": int(workflow_def["timeout"])
    }
    logging.info('merged_payload: %s', merged_payload)
    return merged_payload
