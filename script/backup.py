import os
import logging
import tempfile
import json
from arcgis import GIS
from datetime import datetime
from minio import Minio
from minio.error import S3Error

SRS_AUTH_USR = os.environ['SRS_AUTH_USR']
SRS_AUTH_PSW = os.environ['SRS_AUTH_PSW']
OBJECTSTORE_URL = os.environ['OBJECTSTORE_URL']
OBJECTSTORE_BUCKET =os.environ['OBJECTSTORE_BUCKET']
OBJECTSTORE_KEY = os.environ['OBJECTSTORE_KEY']
OBJECTSTORE_SECRET_KEY = os.environ['OBJECTSTORE_SECRET_KEY']
OBJECTSTORE_FOLDER = os.environ['OBJECTSTORE_FOLDER']
AGO_FOLDER = os.environ['AGO_FOLDER']
# Set logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
#log.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',level=logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
ch.setFormatter(formatter)
log.addHandler(ch)

log.debug('Connecting to ArcGIS Online')
mh = GIS(username=os.environ['SRS_AUTH_USR'],password=os.environ['SRS_AUTH_PSW'])
log.debug('Connected')

def create_temp_backup(item):
    item_data = item.get_data()
    if isinstance(item_data, dict):
        log.debug(f'item.get_data --> is json')
        with tempfile.NamedTemporaryFile(mode="w",delete=False) as tfile: 
            json.dump(item_data, tfile)
        log.debug(tfile.name)
        fname = f"item_{item.id}.json"
        assert os.path.exists(tfile.name)
        outfile = os.path.join(os.path.split(tfile.name)[0],fname)
        os.rename(tfile.name,outfile)
        return outfile
    else:
        log.debug((f'item.get_data --> {item_data}'))
        return item_data

def upload_files_to_objectstore(file_list):
    # upload a list of files to objectstore
    log.debug('Connecting to object storage')
    objstore = Minio(OBJECTSTORE_URL,
        access_key = OBJECTSTORE_KEY,
        secret_key = OBJECTSTORE_SECRET_KEY,)

    log.debug('Object store connected')
    sub_folder = f"bkup_{datetime.today().strftime('%d%m%Y')}"
    for ul_file in file_list:
        file_name = os.path.basename(ul_file)
        log.debug(f'Upload bkup to storage: {OBJECTSTORE_FOLDER}/{sub_folder}/{file_name}')
        objstore.fput_object(
            OBJECTSTORE_BUCKET, f'{OBJECTSTORE_FOLDER}/{sub_folder}/{file_name}', ul_file,
        )
        log.debug('Upload complete')

def get_folder_items(target_folder,bucket, backup_folder):
    items = mh.users.me.items(folder=target_folder)
    local_files = []
    for item in items:
        # download and rename local file
        log.debug(f'Backup {item["name"]}({item["title"]})')
        file = create_temp_backup(item)
        local_files.append(file)
    return local_files

folder = [f for f in mh.users.me.folders if f.get('title')==AGO_FOLDER][0]
bkup_files = get_folder_items(target_folder=folder,bucket=OBJECTSTORE_BUCKET,backup_folder='srs_backups')
upload_files_to_objectstore(bkup_files)
