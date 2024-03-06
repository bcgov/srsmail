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
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = logging.FileHandler(r'SRSMAIL.LOG')
fh.setLevel(logging.INFO)
formatter = logging.Formatter(fmt='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
logger.info('Starting srsmail')
logger.debug('import GIS')
from arcgis.gis import GIS
logger.debug('GIS imported')
from datetime import datetime
from datetime import timezone
this_run = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
# Constants
USER = os.environ['SRS_AUTH_USR']
AUTH = os.environ['SRS_AUTH_PSW']
ITEM = os.environ['SRS_ITEM']
RESOURCE_ITEM = os.environ.get('RESOURCE_ITEM')
SMTP_HOST = os.environ['SMTP_HOST']
CLIENT_EXPERIENCE_DS = os.environ.get('CLIENT_EXPERIENCE_DS')
CLIENT_URL_ROOT = os.environ.get('CLIENT_URL_ROOT')
CLIENT_AGO_ACCOUNT = os.environ.get('CLIENT_AGO_ACCOUNT')
CLIENT_AGO_SECRET = os.environ.get('CLIENT_AGO_SECRET')
CLIENT_REQUEST_URL = os.environ.get('CLIENT_REQUEST_URL')
FROM_EMAIL = os.environ.get('FROM_EMAIL')
URGENT_EMAIL = os.environ.get('URGENT_EMAIL')
TEST_EMAIL = os.environ.get('TEST_EMAIL')

db = os.environ['DB_PATH']
home_path = os.path.dirname(__file__)
logger.debug('Environment read')
logger.debug(f'Current directory:{os.getcwd()}')
if not os.path.exists(db):
    logger.debug(f'Init db for first time:\n{db}')
    con = duckdb.connect(db)
    con.sql("SET Timezone = 'UTC'")
    con.sql('CREATE TABLE request_tracker (request_id VARCHAR PRIMARY KEY,\
             email_ind VARCHAR, email_timestamp TIMESTAMP,lead_resource VARCHAR, lead_email VARCHAR)')
    con.sql('CREATE TABLE monitor(activity_time TIMESTAMP)')
    con.sql('INSERT INTO monitor VALUES (get_current_timestamp())')
    last_run = '2024-01-22 00:00:00'
else:
    logger.debug(f'Reading {db}')
    con = duckdb.connect(db)
    last_run = con.sql('SELECT max(activity_time) last_activity from monitor').fetchone()[0].strftime('%Y-%m-%d %H:%M:%S')
    logger.debug(f'last run: {last_run}')

this_run = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
srs = GIS(username=USER,password=AUTH)
item = srs.content.get(ITEM)
logger.debug('Item content aquired')

def manage_resource_changes():
    # if resource asignment has been added send email
    logger.debug('checking for resource changes')
    unassigned_list = con.sql("SELECT request_id from request_tracker where lead_resource is Null").df().to_dict()['request_id'].values()
    if len(unassigned_list)>0:
        unassigned = ','.join([f"'{i}'" for i in unassigned_list])
        logger.debug(f"found {len(unassigned_list)} requests with unassigned lead: {unassigned}")
        project_resource_view = srs.content.get(RESOURCE_ITEM).tables[0]
        data = project_resource_view.query(where=f"Project_Number IN ({unassigned}) and Resource_Name IS NOT NULL and Resource_Type='Coordinator'",
                                    out_fields="*",
                                    return_all_records=True,return_geometry=False)
        if len(data.features)==0: logger.debug('No new Project Lead assignements')
        for r in data:
            # email client regarding team lead assignment
            logger.info(f"{r.attributes['Project_Number']}: Sending request leader update to: {r.attributes['Client_Email']}")
            attributes = r.attributes
            attributes['Date_Requested']= datetime.fromtimestamp(attributes['Date_Requested'] / 1e3).strftime('%Y-%m-%d')
            attributes['Date_Required']= datetime.fromtimestamp(attributes['Date_Required'] / 1e3).strftime('%Y-%m-%d')
            attributes['CLIENT_AGO_ACCOUNT']= CLIENT_AGO_ACCOUNT
            attributes['CLIENT_AGO_SECRET']= CLIENT_AGO_SECRET
            attributes['CLIENT_REQUEST_URL']= CLIENT_REQUEST_URL
            request_url = f'{CLIENT_URL_ROOT}?data_filter={CLIENT_EXPERIENCE_DS}%3A\
                lower%28Client_Email%29%3D%27{attributes.get("Client_Email")}%27\
                &data_id={CLIENT_EXPERIENCE_DS}%3A{attributes.get("OBJECTID")}&org=governmentofbc'.replace(' ','')
            html = render_template('gss_update.j2', request=attributes,
                            url = request_url)
            if TEST_EMAIL:
                tomail = TEST_EMAIL
            else:
                tomail= r.attributes['Client_Email']
            if tomail is not None:
                send_email(to=tomail,sender=FROM_EMAIL,subject= f"Gespatial Service Request Update[{r.attributes['Project_Number']}]",
                       body=html)
                resource_name = r.attributes['Resource_Name'].replace("'"," ")
                sql = f"UPDATE request_tracker SET lead_resource='{resource_name}', \
                    lead_email='{r.attributes['Resource_Contact_Email']}' \
                    where request_id = '{r.attributes['Project_Number']}'"
                con.sql(sql)
                logger.debug(f'Email sent to: {tomail}')
        return {'updated_cnt':len(unassigned_list)}
    else:
        return {'updated_cnt': 0}


def add_new_request(request_id,email_ind,email_timestamp,lead_name,lead_email):
    values = [request_id,email_ind,email_timestamp,lead_name,lead_email]
    sql = f"INSERT INTO request_tracker VALUES(?,?,?,?,?)"
    con.execute(sql,values)

def request_is_new(request_id):
    sql = f"SELECT request_id from request_tracker where request_id='{request_id}'"
    ids = con.sql(sql).to_df().to_dict()
    if len(ids['request_id'].values()):
        return False
    else:
        return True

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

    # from email.mime.image import MIMEImage

    msg = MIMEMultipart('alternative')
    msg['From']    = sender
    msg['Subject'] = subject
    msg['To']      = to
    msg['Cc']      = cc
        
    # with open('./template/geobc.png','rb') as f:
    #     msgImage = MIMEImage(f.read())
    # msgImage.add_header('Content-ID', '<geobc>')
    # msgImage.add_header('Content-Disposition', 'inline', filename='geobc.png')
    # msg.attach(msgImage)
    toaddrs = []
    if to:
        tolist = to.split(';')
        toaddrs = toaddrs + tolist
    if cc:
        cclist = cc.split(';')
        toaddrs= toaddrs + cclist
    if bcc:
        bcclist = bcc.split(';')
        toaddrs= toaddrs + bcclist
    
    msg.attach(MIMEText(body, 'html'))
    server = smtplib.SMTP(SMTP_HOST)
    response = False
    try:
        logger.debug(f'sending email: {subject}')
        server.sendmail(sender, toaddrs, msg.as_string())
        response = True
    except Exception as e:
        logger.error('Error sending email')
        logger.exception(str(e))
        response = False
    finally:
        server.quit()
        return response

gss_project_table = item.tables[0]

fields = gss_project_table.properties.fields
field_names = [f['name'] for f in fields]
assert ['GlobalID','Date_Requested'] <= field_names
sql = f"Date_Requested BETWEEN TIMESTAMP '{last_run}' AND TIMESTAMP '{this_run}'"
records = gss_project_table.query(where=sql,out_fields="*",return_all_records=True,return_geometry=False)
logger.debug(f'Found {len(records)} requests requiring email')
for r in records.features:
    attributes = r.attributes
    if request_is_new(attributes.get('Project_Number')):
        response = False
        attributes['Date_Requested']= datetime.fromtimestamp(attributes['Date_Requested'] / 1e3).strftime('%Y-%m-%d')
        attributes['Date_Required']= datetime.fromtimestamp(attributes['Date_Required'] / 1e3).strftime('%Y-%m-%d')
        attributes['CLIENT_AGO_ACCOUNT']= CLIENT_AGO_ACCOUNT
        attributes['CLIENT_AGO_SECRET']= CLIENT_AGO_SECRET
        attributes['CLIENT_REQUEST_URL']= CLIENT_REQUEST_URL
        # request_url = f'{CLIENT_URL_ROOT}%3A{attributes.get("OBJECTID")}'
        request_url = f'{CLIENT_URL_ROOT}?data_filter={CLIENT_EXPERIENCE_DS}%3A\
            lower%28Client_Email%29%3D%27{attributes.get("Client_Email")}%27\
            &data_id={CLIENT_EXPERIENCE_DS}%3A{attributes.get("OBJECTID")}&org=governmentofbc'.replace(' ','')
        html = render_template('gss_response.j2', request=attributes,
                            url = request_url)
        if attributes['Priority_Level']== "Urgent":
            logger.info(f"Urgent Request: {attributes['Project_Number']}")
            cc = URGENT_EMAIL
        else:
            cc = None

        if TEST_EMAIL and r.attributes['Project_Number'] is not None:
            email = TEST_EMAIL
            response = send_email(to=email,sender=FROM_EMAIL,subject= f"[TEST] Gespatial Service Request [{r.attributes['Project_Number']}]",body=html)
        elif '@gov.bc.ca' in r.attributes['Client_Email'] and r.attributes['Project_Number'] is not None:
            if r.attributes['Priority_Level'] == 'Urgent':
                email = f"{r.attributes['Client_Email']};{URGENT_EMAIL}"
            else:
                email = r.attributes['Client_Email']
            logger.info(f"{r.attributes['Project_Number']}: Sending mail to {email}")
            response = send_email(to=email,sender=FROM_EMAIL,subject= f"Geospatial Service Request [{r.attributes['Project_Number']}]",body=html)
        else:
            if r.attributes['Project_Number'] is not None:
                logger.info(f"No project number for request {r.attributes['OBJECTID']}")
            else:
                logger.info(f"No confirmaion sent: Non-government Email ({r.attributes['Client_Email']})")
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        if response:
            add_new_request(request_id=attributes['Project_Number'],email_ind="y",
                        email_timestamp=timestamp,lead_name=attributes.get('Project_Lead'),lead_email=attributes.get('Project_Lead_Email'))
        

manage_resource_changes()
# add activity log
r = con.sql('INSERT INTO monitor VALUES (get_current_timestamp())')
logger.info('Mailing complete')