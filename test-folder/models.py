"""
DynamoDB CRUD operations.
"""

import json
import datetime
import logging

from boto3.dynamodb.conditions import Attr

# Setting the log format
FMT="%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
logging.basicConfig(format=FMT, level=logging.INFO, force=True)

# pylint: disable=C0301
def save_job(db_client, sqs_client, data) -> str:
    """
    This function save job to job-details table and then pass data to SQS queue.
    """
    logging.info('data: %s', data)
    table = db_client.Table("job-details")
    queue = sqs_client.get_queue_by_name(QueueName='job_queue')
    job_id = data["job_id"]
    data["job_submitted_time"] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    data["job_state"] = "QUEUED"
    table.put_item(Item=data)
    queue_response = queue.send_message(MessageBody=json.dumps(data))
    logging.info('queue_response: %s', queue_response)
    logging.info('job_id: %s', job_id)
    return job_id


def update_inventory(table, material_id, coupon_quantity) -> None:
    """
    This function updates the coupon_quantity in raw-material-inventory table.
    """
    logging.info('material_id: %s', material_id)
    logging.info('coupon_quantity: %s', coupon_quantity)
    res = table.update_item(
        Key={
            'material_id': material_id,
        },
        UpdateExpression="SET coupon_quantity = coupon_quantity - :val",
        ExpressionAttributeValues={
            ':val': coupon_quantity,
        },
        ReturnValues="UPDATED_NEW"
    )
    logging.info('res: %s', res)


def get_latest_workflow(records) -> dict:
    """
	Get latest workflow definition.
	"""
    max_version = records["Items"][0]["version"].split(".")
    max_version = list(map(int, max_version))
    max_version_workflow = records["Items"][0]
    for record in records["Items"]:
        current_version = record["version"].split(".")
        current_version = list(map(int, current_version))
        if max_version <= current_version:
            max_version = current_version
            max_version_workflow = record
    logging.info('max_version_workflow: %s', max_version_workflow)
    return max_version_workflow


def get_workflow_definition(db_client, data) -> dict:
    """
    Get the workflow defintion based on workflow name and version.
    """
    table = db_client.Table("workflows")
    res = {}
    logging.info('data: %s', data)
    if ("workflow_version" not in data.keys()) or (data["workflow_version"] == ""):
        records = table.scan(FilterExpression=Attr("name").eq(data["workflow"]))
        if records["Count"]>0:
            res = get_latest_workflow(records)
        else:
            raise Exception("Invalid Workflow Name")
    else:
        records = table.scan(FilterExpression=Attr("name").eq(data["workflow"]) & Attr("version").eq(data["workflow_version"]))
        if records["Count"]>0:
            res = records["Items"][0]
        else:
            raise Exception("Invalid Workflow Name or Version")
    logging.info('res: %s', res)
    return res
