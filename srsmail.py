# Author: wburt
# Ministry, Division, Branch: GeoBC
# Created Date: 2022-01-23
# Updated Date: 
# Description:
#     This script is run to detect new records in a feature service and send an 
#     confirmation email to the recipient

# --------------------------------------------------------------------------------

# - INPUTS: Inputs are controled by the environment variables assigned to globals

# - OUTPUTS: html rendered email

# --------------------------------------------------------------------------------
# * IMPROVEMENTS
# * Suggestions...
# --------------------------------------------------------------------------------
# * HISTORY

#   Date      Initial/IDIR  Description
# | ------------------------------------------------------------------------------
#   2024-01-23   wb       Init

import os
import duckdb

import jinja2
import smtplib
import logging
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',level=logging.DEBUG)
logging.info('Starting srsmail')
logging.debug('import GIS')
from arcgis import GIS
logging.debug('GIS imported')
from datetime import datetime
from datetime import timezone
this_run = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
# Constants
USER = os.environ['SRS_AUTH_USR']
AUTH = os.environ['SRS_AUTH_PSW']
ITEM = os.environ['SRS_ITEM']
SMTP_HOST = os.environ['SMTP_HOST']
FROM_EMAIL = os.environ.get('FROM_EMAIL')
TEST_EMAIL = os.environ.get('TEST_EMAIL')

db = os.environ['DB_PATH']
home_path = os.path.dirname(__file__)
logging.debug('Environment read')
if not os.path.exists(db):
    logging.debug(f'Init db for first time:\n{db}')
    con = duckdb.connect(db)
    con.sql("SET Timezone = 'UTC'")
    con.sql('CREATE TABLE request_tracker (request_id VARCHAR PRIMARY KEY,\
             email_ind VARCHAR, email_timestamp TIMESTAMP)')
    con.sql('CREATE TABLE monitor(activity_time TIMESTAMP)')
    con.sql('INSERT INTO monitor VALUES (get_current_timestamp())')
    last_run = '2024-01-22 00:00:00'
else:
    logging.debug(f'Reading {db}')
    con = duckdb.connect(db)
    last_run = con.sql('SELECT max(activity_time) last_activity from monitor').fetchone()[0].strftime('%Y-%m-%d %H:%M:%S')
    logging.debug(f'last run: {last_run}')

this_run = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
srs = GIS(username=USER,password=AUTH)
item = srs.content.get(ITEM)
logging.debug('Item content aquired')

def render_template(template, request,url):
    ''' renders a Jinja template into HTML '''
 
    templateLoader = jinja2.FileSystemLoader(searchpath=os.path.join(home_path,"template"))
    templateEnv = jinja2.Environment(loader=templateLoader)
    templ = templateEnv.get_template(template)
    return templ.render(request=request,url=url)


def send_email(to, sender='NoReply@geobc.ca>',
                cc=None, bcc=None, subject=None, body=None):
    ''' sends email using a Jinja HTML template '''
    # Import the email modules
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.header import Header
    from email.utils import formataddr
    
    msg = MIMEMultipart('alternative')
    msg['From']    = sender
    msg['Subject'] = subject
    msg['To']      = to
    msg['Cc']      = cc
    msg['Bcc']     = bcc
    msg.attach(MIMEText(body, 'html'))
    server = smtplib.SMTP(SMTP_HOST)
    try:
        logging.info(f'sending email: {subject}')
        server.sendmail(sender, to, msg.as_string())
    except Exception as e:
        logging.error('Error sending email')
        logging.exception(str(e))
    finally:
        server.quit()

gss_project_table = item.tables[0]
field_names = [f['name'] for f in gss_project_table.properties.fields]
assert ['GlobalID','Date_Requested'] <= field_names
sql = f"Date_Requested BETWEEN TIMESTAMP '{last_run}' AND TIMESTAMP '{this_run}'"
records = gss_project_table.query(where=sql,out_fields="*",return_all_records=True,return_geometry=False)
for r in records.features:
    html = render_template('gss_response.j2', request=r.attributes,
                           url = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ')

    if r.attributes['Project_Number'] is None:
        proj_num = 'No project'
    else:
        proj_num = r.attributes['Project_Number']
    if TEST_EMAIL:
        email = TEST_EMAIL
        send_email(to=email,subject=r.attributes['Project_Number'],body=html)
    elif '@gov.bc.ca' in r.attributes['Client_Email']:
        email = r.attributes['Client_Email']
        send_email(to=email,subject=r.attributes['Project_Number'],body=html)
        sql = f"INSERT INTO request_tracker VALUES ('{proj_num}','{r.attributes['Client_Email']}', get_current_time());"
        con.sql(sql)
    else:
        logging.info(f"No confirmaion sent: Non-government Email ({r.attributes['Client_Email']})")

con.sql('INSERT INTO monitor VALUES (get_current_timestamp())')