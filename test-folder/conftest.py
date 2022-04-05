# pylint: disable-all

import pytest
import boto3
import datetime
from moto import mock_sqs, mock_dynamodb2, mock_iot

current_time = datetime.datetime.utcnow()

@pytest.fixture
def dynamodb_client():
    with mock_dynamodb2():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table1 = dynamodb.create_table(
            TableName='job-details',
            KeySchema=[{'AttributeName': 'job_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'job_id','AttributeType': 'S'},{'AttributeName': 'job_state', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "job_state_index",
                    "KeySchema": [{'AttributeName': 'job_state', 'KeyType': 'HASH'}],
                    "Projection": {
                        "ProjectionType": "ALL",
                    },
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ]
        )

        table1.put_item(
            Item={
                    "instance": "zach",
                    "job_id": "1e9eb16b-d416-11eb-9168-058375ca72aa",
                    "job_requested_start_time": str((current_time + datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")),
                      "job_state": "QUEUED"
                }
        )

        table2 = dynamodb.create_table(
            TableName='workflows',
            KeySchema=[{'AttributeName': 'name', 'KeyType': 'HASH'}, {'AttributeName': 'version', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': 'name','AttributeType': 'S'}, {'AttributeName': 'version', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )

        table2.put_item(
            Item={
                "description": "This workflow defines the individual steps necessary to process coupons through the preparation process",
                "name": "coupon-prep",
                "steps": [
                    {
                    "function": "loader",
                    "step_data": {
                        "properties": {
                        "coupon_material": {
                            "description": "Number of items that need to be loaded",
                            "type": "string"
                        },
                        "coupon_quantity": {
                            "description": "Number of items that need to be loaded",
                            "type": "integer"
                        },
                        "job_requested_start_time": {
                            "pattern": "^([0-9]{4})-?(1[0-2]|0[1-9])-?(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):?([0-5][0-9]):?([0-5][0-9])Z$",
                            "type": "string"
                        }
                        },
                        "required": [
                        "coupon_material",
                        "coupon_quantity",
                        "job_requested_start_time"
                        ],
                        "type": "object"
                    }
                    },
                    {
                    "function": "conveyer",
                    "step_data": {
                        "properties": {
                        "coupon_quantity": {
                            "description": "Number of items that need to be loaded",
                            "type": "integer"
                        }
                        },
                        "type": "object"
                    }
                    },
                    {
                    "function": "abrader",
                    "step_data": {
                        "properties": {
                        "coupon_material": {
                            "description": "Number of items that need to be loaded",
                            "type": "string"
                        },
                        "coupon_quantity": {
                            "description": "Number of items that need to be loaded",
                            "type": "integer"
                        }
                        },
                        "type": "object"
                    }
                    },
                    {
                    "function": "conveyer",
                    "process_end": True,
                    "step_data": {
                        "properties": {
                        "coupon_quantity": {
                            "description": "Number of items that need to be loaded",
                            "type": "integer"
                        }
                        },
                        "type": "object"
                    }
                    }
                ],
                "timeout": "300",
                "version": "2.0.0"
            }
        )

        table3 = dynamodb.create_table(
            TableName='raw-material-inventory',
            KeySchema=[{'AttributeName': 'material_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'material_id','AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )

        table3.put_item(
            Item={
                "material_id": "1234456",
                "coupon_description": "",
                "coupon_material": "wood",
                "coupon_quantity": 1000
            }
        )

        yield dynamodb


@pytest.fixture
def sqs_client():
    with mock_sqs():
        conn = boto3.client("sqs", region_name="us-east-1")
        response = conn.create_queue(QueueName='job_queue')
        queue_url = response['QueueUrl']
        yield conn

@pytest.fixture
def iot_client():
    with mock_iot():
        conn = boto3.client("iot", region_name="us-east-1")
        conn.create_thing(
            thingName="TestThing_1",
            attributePayload={
                "attributes": {
                    "instance": "zach",
                    "workflow": "coupon-prep"
                }
            }
        )
        conn.create_thing(
            thingName="TestThing_2"
        )
        yield conn
