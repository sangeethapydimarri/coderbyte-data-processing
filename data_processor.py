import json

import boto3
from dateutil import parser


def validate_meteor_data(meteor_data: dict):
    """
    Validate if the message has all necessary fields to correctly calculate average mass and max meteor fall year

    :raises ValueError:
        Raised if required fields are missing
    """
    if not meteor_data.get('year', None):
        raise ValueError("Incomplete dataset, missing year")
    if not meteor_data.get('mass', None):
        raise ValueError("Incomplete dataset, missing mass")
    if not meteor_data.get('fall', None):
        raise ValueError("Incomplete dataset, missing fall")


if __name__ == '__main__':
    bucket = boto3.resource('s3').Bucket('majorly-meteoric')
    per_year_meteor_counter = {}
    total_mass = 0.0
    total_meteor_counter = 0
    for obj in bucket.objects.all():
        key = obj.key
        body = obj.get()['Body'].read()
        try:
            # decode json list bytes into json list. Handle exception if any {JSONDecodeError}
            meteor_data_list = json.loads(body.decode("utf-8"))
            for each_meteor_details in meteor_data_list:
                # validate each message before processing
                validate_meteor_data(each_meteor_details)

                # calculate average for meteor falls only
                if each_meteor_details['fall'] == 'Fell':
                    meteor_fall_datetime = parser.parse(each_meteor_details['year'])
                    current_meteor_year_counter = per_year_meteor_counter.get(meteor_fall_datetime.year, None)
                    # update per year meteor counter
                    if current_meteor_year_counter:
                        per_year_meteor_counter[meteor_fall_datetime.year] = current_meteor_year_counter + 1
                    else:
                        per_year_meteor_counter[meteor_fall_datetime.year] = 1

                    total_meteor_counter += 1
                    total_mass += float(each_meteor_details['mass'])
        except Exception as e:
            print(f"Error processing file: {key} error: {e}")

    max_count = max(per_year_meteor_counter.values())
    max_meteor_year_list = [k for k, v in per_year_meteor_counter.items() if v == max_count]

    print(f"Year's with maximum meteor fall: {max_meteor_year_list}")
    print(f"Average mass of a meteor fall: {total_mass / total_meteor_counter}")
