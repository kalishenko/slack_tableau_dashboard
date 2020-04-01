#!/usr/bin/python
import logging
import requests
import os
import pandas as pd
import snowflake.connector
import time
from gspread_pandas import Spread
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from src.utils import properties as de_properties
from src.utils.connections.DBSnowflake import DBSnowflake

log = logging.getLogger(__name__)
keys = de_properties.get_config_section('sheets_keys')

'''
## PUBLIC CHANNEL - FOR PRODUCTION
alert_webhook = "https://hooks.slack.com/services/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"
incident_webhook = "https://hooks.slack.com/services/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"
positive_webhook = "https://hooks.slack.com/services/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"
'''

## PRIVATE MESSAGE TO RODOSHI - FOR TESTING
alert_webhook = "https://hooks.slack.com/services/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"
incident_webhook = "https://hooks.slack.com/services/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"
positive_webhook = "https://hooks.slack.com/services/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"


## ALERT CONFIGURATION INPUT GSHEET
## inputspread = Spread(keys.get('metric_alerts_input')) 
## inputworksheet = inputspread.sheet_to_df(index=0, header_rows=1, start_row=1, sheet="Config")

## INCIDENTS OUTPUT GSHEET
outputspread = Spread(keys.get('metric_alerts_output')) 
gsheet_url = "https://docs.google.com/spreadsheets/d/" + keys.get('metric_alerts_output') + "/edit?usp=sharing"

## FOR THE CURSOR ONLY
cursor_properties = de_properties.get_config_section('snowflake')

cursor_username = '{username}'.format(**cursor_properties)
cursor_password = '{password}'.format(**cursor_properties)
cursor_account = '{account}'.format(**cursor_properties)
cursor_warehouse = '{datawarehouse}'.format(**cursor_properties)
cursor_schema = '{schema}'.format(**cursor_properties)
cursor_database = '{database}'.format(**cursor_properties)

def query_snowflake():
    log.info("Init Snowflake connection")
    engine = DBSnowflake('snowflake').engine
  
    try:
            
        connection = engine.connect()
             
        ## UPDATE ALERT CONFIGURATION
        ## inputworksheet.to_sql('metric_alerts_config', con=connection, schema= 'staging', index=False, if_exists="replace")

        ## STORED PREOCURE TO REFRESH DATA
        connection.execute("call util_schema.execute_metric_alerts_data_refresh()")
        
        time.sleep(900)

        print("Data Refresh Complete!")

        ## SEND THE INCIDENTS TO GSHEETS    
        ## gsheet_output = "SELECT date, metric, dimension, dimension_value, CASE WHEN metric like '%Rate%' THEN CAST(ROUND(metric_value*100,2) as varchar) || '%' ELSE CAST(ROUND(metric_value,0) as varchar) END AS metric_value, CASE WHEN metric like '%Rate%' THEN CAST(ROUND(dow_avg_value*100,2) as varchar) || '%' ELSE CAST(ROUND(dow_avg_value,0) as varchar) END AS avg_value, CAST(ROUND(percentage_difference*100,2) as varchar) || '%' as percentage_difference, CURRENT_DATE as alerted_date, metric_owner FROM edw.v_ma_current_alerts WHERE second_alert = 'Y';"
        ## incidents = pd.read_sql_query(gsheet_output, connection)

        ## FIND THE FIRST EMPTY CELL
        ## outputworksheet = outputspread.sheet_to_df(index=1, header_rows=0, start_row=3, sheet="Incidents")
        ## filledrows = len(outputworksheet.index)
        ## startnum = filledrows+3
        ## startcell = "A" + str(startnum)   

        ## SEND OUTPUT INCIDENTS
        ## try:
            ## outputspread.df_to_sheet(incidents, headers=False, index=False, sheet="Incidents", start=startcell, replace=False)
            ## print("Incident Log Updated!")
        ## except: 
            ## print("No Incidents!") 
        
        ## SEND SLACK ALERTS

        connection2 = snowflake.connector.connect(
                        user = cursor_username,
                        password = cursor_password, 
                        account = cursor_account,
                        warehouse = cursor_warehouse,
                        schema = cursor_schema,
                        database = cursor_database,
                        )
        
        metric_alerts_cursor = connection2.cursor()
        metric_alerts_cursor.execute("SELECT DISTINCT metric, dimension, date, first_alert, second_alert, positive_alert, metric_owner_id, dashboard FROM edw.v_ma_current_alerts WHERE first_alert = 'Y' OR second_alert = 'Y' OR positive_alert = 'Y' ORDER BY date, metric, dimension;")
        metric_alerts = metric_alerts_cursor.fetchall()    
        
        ## OBTAIN THE DETAILS FOR EACH ALERT
        for row in metric_alerts:
                
            metric = row[0]
            dimension = row[1]
            date = row[2]
            first_alert = row[3]
            second_alert = row[4]
            positive_alert = row[5]
            user_id = row[6]
            dashboard = row[7]
      
            alert_sql = "SELECT dimension_value, metric_value, dow_avg_value, percentage_difference FROM edw.v_ma_current_alerts WHERE first_alert = '" +  first_alert + "' AND second_alert = '" + second_alert + "' AND positive_alert = '" + positive_alert + "' AND metric = '" + metric + "' AND dimension = '" + dimension + "' ORDER BY std_dev" 
                
            alerts_cursor = connection2.cursor()
            alerts_cursor.execute(alert_sql)
            
            alerts = alerts_cursor.fetchall()
            
            values = ''

            ## alert_user = user_id ## COMMENT OUT FOR TESTING 

            alert_user = '' ## COMMENT OUT FOR PRODUCTION
            
            for row in alerts:
                
                dimension_value = row[0]
                metric_value = row[1]
                avg_value = row[2]
                percentage_diff = row[3]
                       
                ## FORMATTING ALERTS
            
                metric_text = metric.lower()
                percentage_diff = round(percentage_diff*100,2)
                
                if "Rate" in metric:
                    metric_value = round(metric_value*100,2)
                    metric_value = str(metric_value) + "%"
                    avg_value = round(avg_value*100,2)
                    avg_value = str(avg_value) + "%"
                
                if "Rate" not in metric:
                    metric_value = round(metric_value,0)
                    avg_value = round(avg_value,0)
            
                values = values + "\n•  *" + dimension_value + "* - _*" + str(metric_value) + "*_, the average is _*" + str(avg_value) + "*_, changed by _*" + str(percentage_diff) + "%*_"
                
            ## FORMAT THE MESSAGE ACCORDING TO THE ALERT 
                
            line_1 = "\n>_" + date.strftime('%d %B %Y') + "_" 
            line_2 = "\n>*" + metric + "* for *" + dimension + "*"
            line_3 = values
            line_4 = "\n\n <@" + str(alert_user) + ">\n\n<" 
            line_5 = gsheet_url + "|Provide Details :paperclip:>   <" 
            line_6 = str(dashboard) + "|View Dashboard> :chart_with_downwards_trend:" 
            
            header = ":exclamation: _*Alert*_" 
            message = line_1 + line_2 + line_3 + line_4 + line_6
            webhook = alert_webhook

            if positive_alert == "Y": 
                header = ":awesome::francois: _*Great Stuff!*_"
                webhook = positive_webhook
           
            if second_alert == "Y": 
                message = line_1 + line_2 + line_3 + line_4 + line_5 + line_6
                webhook = incident_webhook
            
            ## SEND THE POST REQUEST TO SLACK    
            
            post = {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": header
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message
                    }
                },
                {
                    "type": "divider"
                }
            ]
            }
            
            response = requests.post(         
                    url = webhook,
                    json = post
            )    

            print("Alert Sent!")    
        
    finally: #CLOSE YOUR CONNECTION!
        connection.close()
        engine.dispose()

        connection2.close()

if __name__ == "__main__":
    query_snowflake()
