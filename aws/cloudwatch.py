from datetime import datetime
import boto3

from streaming.constants import CLOUD_WATCH_NAMESPACE, CLOUD_WATCH_METRIC_NAME, CLOUD_WATCH_DIMENSION_NAME

cloud_watch = boto3.client('cloudwatch')


def push_to_cloud_watch_metrics(tweet_country):
    metric_data = [
        {
            'MetricName': CLOUD_WATCH_METRIC_NAME,
            'Dimensions': [
                {
                    'Name': CLOUD_WATCH_DIMENSION_NAME,
                    'Value': tweet_country
                }
            ],
            'Timestamp': datetime.now(),
            'Value': 1
        }
    ]
    try:
        cloud_watch.put_metric_data(
            Namespace=CLOUD_WATCH_NAMESPACE,
            MetricData=metric_data
        )
    except Exception as e:
        print("Error on pushing to cloud_watch metrics ", str(e))
        pass


def list_cloud_watch_metrics(dimension_name):
    paginator = cloud_watch.get_paginator('list_metrics')
    metrics = []
    for response in paginator.paginate(Dimensions=[{'Name': dimension_name}],
                                       MetricName=CLOUD_WATCH_METRIC_NAME,
                                       Namespace=CLOUD_WATCH_NAMESPACE):
        metrics.append(response['Metrics'])
    return metrics



