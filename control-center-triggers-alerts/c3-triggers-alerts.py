import yaml
import requests


def create_trigger(control_center_url, auth_type, username, password, cert_file, trigger):
    url = f"{control_center_url}/api/v1/alerts/create-trigger"
    headers = {'Content-Type': 'application/json'}

    if auth_type == 'username_password':
        auth = (username, password)
    elif auth_type == 'certificate':
        auth = (cert_file, cert_file)
    else:
        raise ValueError('Invalid auth_type specified in the configuration.')

    data = {
        "triggerName": trigger['name'],
        "condition": f"${trigger['event']} > {trigger['threshold']}",
        "enabled": True,
        "actions": []
    }

    response = requests.post(url, json=data, headers=headers, auth=auth)
    if response.status_code == 201:
        trigger_id = response.json().get('id')
        print(f"Trigger '{trigger['name']}' created successfully with ID: {trigger_id}")
        return trigger_id
    else:
        print(f"Failed to create trigger '{trigger['name']}'. Response: {response.content}")


def create_alert(control_center_url, auth_type, username, password, cert_file, trigger_id, trigger):
    url = f"{control_center_url}/api/v1/alerts/create-alert"
    headers = {'Content-Type': 'application/json'}

    if auth_type == 'username_password':
        auth = (username, password)
    elif auth_type == 'certificate':
        auth = (cert_file, cert_file)
    else:
        raise ValueError('Invalid auth_type specified in the configuration.')

    data = {
        "alertName": trigger['name'] + "_alert",
        "triggerId": trigger_id,
        "enabled": True,
        "actions": []
    }

    response = requests.post(url, json=data, headers=headers, auth=auth)
    if response.status_code == 201:
        print(f"Alert for trigger '{trigger['name']}' created successfully.")
    else:
        print(f"Failed to create alert for trigger '{trigger['name']}'. Response: {response.content}")


def main():
    with open('config.yaml') as config_file:
        config = yaml.safe_load(config_file)

    control_center_url = config['control_center']['url']
    auth_type = config['control_center']['auth_type']
    username = config['control_center']['username']
    password = config['control_center']['password']
    cert_file = config['control_center'].get('cert_file', None)

    with open('triggers.yaml') as triggers_file:
        triggers = yaml.safe_load(triggers_file)

    for trigger in triggers:
        trigger_id = create_trigger(control_center_url, auth_type, username, password, cert_file, trigger)
        if trigger_id is not None:
            create_alert(control_center_url, auth_type, username, password, cert_file, trigger_id, trigger)


if __name__ == '__main__':
    main()
