import yaml
import json
from datetime import datetime
from flask import Request
from pathlib import Path

from BigQuery.BigQuery import BigQuery
from Authentication.Authentication import Authentication

config_path = Path.cwd() / "config.yaml"
CONFIGURATION: dict = {}
with open(config_path) as stream:
    CONFIGURATION = yaml.safe_load(stream)

def geoIncludeAlert(request: Request):
    
    QUERY = f'''
        select 
            cast(_TABLE_SUFFIX as integer) as Adveriser_ID,
            name as Line_Item,
            line_item_id as Line_Item_ID
        from `nyo-yoptima.sdf_line_item.*`
        where 
            geography_targeting___include is null
            and status in ("Active", "Draft")
    '''

    try:
        bq_handler = BigQuery()
        alert_data = bq_handler.runQuery(QUERY)
    except Exception as error:
        return json.dumps({ "Message" : "Error Running Query", "error" : str(error) }), 500
    
    if len(alert_data) == 0:
        return json.dumps({ "Message" : "No Alerts to Report" }), 200

    DATE_STRING = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    EMAIL_ENDPOINT = CONFIGURATION.get("MESSAGE_SERVICE").get("EMAIL_ENDPOINT")
    MESSAGE_JSON = {
        "to_emails": ",".join(CONFIGURATION.get("MESSAGE_SERVICE").get("EMAIL_RECEIVER")),
        "subject": f"Geography Inclusion List Missing ({DATE_STRING})",
        "subtext": "Geography inclusion list is missing for the following Line Items. Please Handle this as soon as possible.",
        "table_data": alert_data,
        "plain_text_data": "",
        "header": "",
        "footer": ""
    }

    try:
        auth_handler = Authentication()
        response = auth_handler.generateAuthenticatedRequest(
            target_url = EMAIL_ENDPOINT,
            method = "POST",
            post_json = MESSAGE_JSON
        )
    except Exception as error:
        return json.dumps({ "message" : "Authenticated Request Failed", "error" : str(error) }), 500
    
    if response.status_code == 200:
        return json.dumps({ "message" : "Alert Successfully Generated" }), 200
    else:
        return json.dumps({ "message" : "Authenticated Request Failed", "error" : f"Status Code :{response.status_code}" }), 500
