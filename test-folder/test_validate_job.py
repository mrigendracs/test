# pylint: disable-all 

import validate_job_http
import pytest
import json
import datetime

current_time = datetime.datetime.utcnow()
job_current_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

@pytest.mark.parametrize("input,expected_response",
       [
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "testcom",
                "user_name": "test",
                "workflow": "couponprep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
         {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"Invalid user_email pattern\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "couponprep",
                "workflow_version": "1.0",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
         {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"Invalid workflow_version pattern\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_name": "test",
                "workflow": "couponprep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
         {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"'user_email' is a required property\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "couponprep",
                "workflow_version": "1.0",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                },
                "test": "test"
            }
        },
         {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"Additional properties are not allowed ('test' was unexpected)\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "couponprep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
         {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"Invalid Workflow Name\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zac",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }

        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": [\"Invalid Instance Name\"]}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1200,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": [\"Available wood coupon quantity is 1000\"]}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wooood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": [\"Available wooood coupon quantity is 0\"]}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": "2021-07-01T15:17:00Z"
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"job_requested_start_time is invalid. Jobs can be scheduled after\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "workflow_version": "2.0.0",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood", 
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
        {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": '{"message": "Job is Successful"}'
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
        {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": '{"message": "Job is Successful"}'
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "workflow_version": "1.0.0",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"Invalid Workflow Name or Version\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zach",
                "job_inputs": {
                    "coupon_quantity": 1,
                    "coupon_material": "wood",
                    "job_requested_start_time": str((current_time + datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": \"job cannot be scheduled at\"}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zac",
                "job_inputs": {
                    "coupon_quantity": 1200,
                    "coupon_material": "wood",
                    "job_requested_start_time": str(job_current_time)
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": [\"Invalid Instance Name\", \"Available wood coupon quantity is 1000\"]}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zac",
                "job_inputs": {
                    "coupon_quantity": 1000,
                    "coupon_material": "wood",
                    "job_requested_start_time": "2021-07-01T15:17:00Z"
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": [\"Invalid Instance Name\", \"job_requested_start_time is invalid. Jobs can be scheduled after\"]}"
        }),
        ({
            "version": "1.0.0",
            "body": {
                "user_email": "test@test.com",
                "user_name": "test",
                "workflow": "coupon-prep",
                "instance": "zac",
                "job_inputs": {
                    "coupon_quantity": 1000,
                    "coupon_material": "wood",
                    "job_requested_start_time": str((current_time + datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
                }
            }
        },
        {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": "{\"errorMessage\": [\"Invalid Instance Name\"]}"
        })
    ])
def test_validate_lambda_handler(dynamodb_client, iot_client, sqs_client, input,expected_response):
    # Converting input body from dict to string for HTTP API Requests
    if "body" in input:
        input["body"] = json.dumps(input["body"])
    actual_response = validate_job_http.lambda_handler(event=input, context={})
    if "job_id" in actual_response:
        actual_response = validate_result(actual_response,expected_response)
    elif "body" in actual_response and "job_id" in actual_response["body"]:
        # Condition for HTTP API Requests
        actual_response["body"] = json.loads(actual_response["body"])
        actual_response = validate_result(actual_response["body"],expected_response)
    elif "job" in actual_response["body"]:
        actual_response["body"] = validate_result(actual_response,expected_response)
    assert actual_response == expected_response

def validate_result(actual_response,expected_response):
    val = -1
    # Filtering the type of request based on job_id
    if "job_id" in actual_response:
        val = expected_response["body"].find(actual_response["message"])
        if val != -1:
           return expected_response
        else:
           return actual_response
    else:
        actual_response["body"] = json.loads(actual_response["body"])
        expected_response["body"] = json.loads(expected_response["body"])
        for i in range(len(actual_response["body"]["errorMessage"])):
            val = actual_response["body"]["errorMessage"][i].find(expected_response["body"]["errorMessage"][i])
            if val == -1:
                return actual_response["body"]
        return expected_response["body"]
