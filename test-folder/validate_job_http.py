"""
Validate job
"""

import os
import datetime
import logging
import uuid
import json
import boto3
from boto3.dynamodb.conditions import Attr
from dateutil import parser
from jsonschema import validate
import models
import payload

# importing logger
FMT="%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
logging.basicConfig(format=FMT, level=logging.INFO, force=True)

# pylint: disable=W0613,C0301,R0914,W0703,E1101,R0915
def validate_time_interval(db_client, instance, job_requested_start_time, timeout=300) -> dict:
    """
    This function validates if the job_requested_start_time is not
    less than workflow timeout of current time and also validates if any
    existing job is not in that timeout window.
    """
    logging.info('job_requested_start_time: %s', job_requested_start_time)
    logging.info('timeout: %s', timeout)
    job_details_tbl = db_client.Table("job-details")
    threshold_interval = datetime.timedelta(seconds=timeout)
    current_time = datetime.datetime.utcnow()
    response = {}

    # Checking whether the job_requested_start_time is not more than 5 mins less than the current time
    if ((job_requested_start_time - current_time).total_seconds()/60) < -5:
        response["message"] = "job_requested_start_time is invalid. Jobs can be scheduled after " + \
            str((current_time - datetime.timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        response["validation"] = False
        logging.info('response: %s', response)
        return response

    # Checking whether the job_requested_start_time is not after 14 days of the current_time
    if ((job_requested_start_time - current_time).days) > 14:
        response["message"] = "job_requested_start_time is invalid. Jobs can be scheduled upto " + \
            str((current_time + datetime.timedelta(days=14)).strftime('%Y-%m-%dT%H:%M:%SZ'))
        response["validation"] = False
        logging.info('response: %s', response)
        return response

    # Checking whether records exists between the defined thresholds
    upper_threshold = (job_requested_start_time + threshold_interval).strftime('%Y-%m-%dT%H:%M:%SZ')
    lower_threshold = (job_requested_start_time - threshold_interval).strftime('%Y-%m-%dT%H:%M:%SZ')
    records = job_details_tbl.scan(
        Select='ALL_ATTRIBUTES',
        FilterExpression=Attr("job_requested_start_time").between(lower_threshold,
            upper_threshold) & Attr("instance").eq(instance)
    )

    validation = len(records["Items"]) == 0
    if validation:
        response["message"] = "job_requested_start_time validation is success."
        response["validation"] = True
        logging.info('response: %s', response)
        return response

    # Finding the maximum time of a job from the list of previously retrieved records
    max_time_in_interval = None
    for item in records["Items"]:
        item_time_in_interval = parser.isoparse(item["job_requested_start_time"]).replace(tzinfo=None)
        if max_time_in_interval is None:
            max_time_in_interval = item_time_in_interval
        else:
            max_time_in_interval = max(
                max_time_in_interval, item_time_in_interval)
    response["message"] = "job cannot be scheduled at " + str(job_requested_start_time) + ". Jobs can be scheduled after " + str(
        (max_time_in_interval + datetime.timedelta(seconds=timeout)).strftime('%Y-%m-%dT%H:%M:%SZ'))
    response["validation"] = False
    logging.info('response: %s', response)
    return response


def lambda_handler(event, context):
    """
    This function validates the job sent by HTTP API.
    """
    db_client = boto3.resource("dynamodb", region_name="us-east-1")
    sqs_client = boto3.resource("sqs", region_name="us-east-1")
    client = boto3.client("iot", region_name="us-east-1")
    logging.info('event: %s', event)
    data = json.loads(event["body"])
    logging.info('data: %s', data)
    validate_messages = []
    try:
        # Retieving json schema and validating Top-level attributes in json input
        base_dir = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(base_dir, "json_schema.json")
        with open(filename, "r", encoding="utf-8") as file_handle:
            schema = json.load(file_handle)
        validate(instance=data, schema=schema)

        # Retrieving workflows definition and validating Bottom-level attributes in json input
        workflow_def = models.get_workflow_definition(db_client, data)
        logging.info('workflow_def: %s', workflow_def)
        for step in workflow_def["steps"]:
            validate(instance=data["job_inputs"], schema=step["step_data"])
        timeout = int(workflow_def["timeout"])

        # Retrieving IoT thing attributes for validation and converting job_requested_start_time
        client_list = client.list_things()
        things_data = client_list["things"]
        attributes = [sub['attributes']["instance"]
                      for sub in things_data if "instance" in sub["attributes"]]
        logging.info('attributes: %s', attributes)
        if data["instance"] not in attributes:
            validate_messages.append("Invalid Instance Name")
        job_requested_start_time = parser.isoparse(data["job_inputs"]['job_requested_start_time']).replace(tzinfo=None)

        # Retrieving details from inventory db for validation
        inventory_table = db_client.Table("raw-material-inventory")
        result = inventory_table.scan(FilterExpression=Attr(
            "coupon_material").eq(data["job_inputs"]['coupon_material']))
        logging.info('result: %s', result)
        has_quantity = 0
        for item in result['Items']:
            has_quantity = has_quantity + item['coupon_quantity']
            material_id = item['material_id']
        logging.info('job_requested_start_time: %s', job_requested_start_time)
        logging.info('timeout: %s', timeout)
        validation_response = validate_time_interval(db_client, data["instance"],
                                                     job_requested_start_time, timeout)
        logging.info('validation_response: %s', validation_response)

        # Checkin whether coupon_quantity and provided start time is valid
        if data["job_inputs"]['coupon_quantity'] > 0 and data["job_inputs"]['coupon_quantity'] <= has_quantity:
            if validation_response["validation"] and not validate_messages:
                job_id = str(uuid.uuid1())
                data["job_id"] = job_id
                payload_data = payload.create_payload(data, workflow_def)
                job_id = models.save_job(db_client, sqs_client, payload_data)
                response = {
                    "statusCode": 200,
                    "headers": {'Content-Type': 'application/json'},
                    "body": {
                        "message": "Job is Successful",
                        "job_id": job_id,
                    }
                }
                logging.info('Updating raw-material-inventory')
                models.update_inventory(inventory_table, material_id,
                                        data["job_inputs"]['coupon_quantity'])
                response["body"] = json.dumps(response["body"])
                logging.info('response: %s', response)
                return response
        else:
            validate_messages.append(
                "Available "+data["job_inputs"]["coupon_material"]+" coupon quantity is "+str(has_quantity))
        if not validation_response["validation"]:
            validate_messages.append(validation_response["message"])
        raise Exception(validate_messages)
    except Exception as exception:
        response_data = {
            "statusCode": 400,
            "headers": {'Content-Type': 'application/json'},
            "body": {
                "errorMessage": exception.args[0]
            }
        }
        if type(exception).__name__ == "ValidationError":
            response_data["body"]["errorMessage"] = exception.message
            # Overiding errorMessage in response_data with a custom message, if defined in json schema
            if "errorMessage" in exception.schema:
                response_data["body"]["errorMessage"] = exception.schema["errorMessage"][exception.validator]
        response_data["body"] = json.dumps(response_data["body"])
        logging.info('response_data: %s', response_data)
        return response_data
