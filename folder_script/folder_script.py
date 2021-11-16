from sqlalchemy import *
import os
import concurrent.futures
import json
from datetime import datetime
from time import sleep


metadata = MetaData()

USERS_TABLE = Table(
    'users', metadata,
        Column('id', Integer, primary_key=True),
        Column('login', Text),
        Column('full_name', Text),
        Column('position', Text),
        Column('director_id', Integer),
        Column('timestamp_document_record', Text),
)

DOCUMENTS_TABLE = Table(
    'documents', metadata,
        Column('id', Integer, primary_key=True),
        Column('timestamp_document_created', Text),
        Column('timestamp_document_changed', Text),
        Column('user_id', Integer),
        Column('data', Text),
)

DATABASE_ENGINE = create_engine()
metadata.create_all(DATABASE_ENGINE)

FILE_NAMES = []


def record_file(file_path):
    global FILE_NAMES # Need for access to privileges of changing object

    if os.path.exists(file_path):

        with open(file_path, 'r') as f:
            json_text = json.loads(f.read())

        os.remove(file_path)
        
        FILE_NAMES.remove(file_path) # Not working without 'global', no access to change the object

        request = DATABASE_ENGINE.execute(USERS_TABLE.select().where(USERS_TABLE.c.login == json_text['login'])).fetchone()

        if not request == None:
            DATABASE_ENGINE.execute(
                USERS_TABLE.update().\
                    values(timestamp_document_record = datetime.now().timestamp()).\
                    where(USERS_TABLE.c.id == request.id)
            )
            primary_key = request.id
        else:
            result = DATABASE_ENGINE.execute(USERS_TABLE.insert().values(
                login = json_text['login'],
                timestamp_document_record = datetime.now().timestamp()
            ))

            primary_key = result.inserted_primary_key

        DATABASE_ENGINE.execute(DOCUMENTS_TABLE.insert().values(
            timestamp_document_created = datetime.now().timestamp(),
            timestamp_document_changed = datetime.now().timestamp(),
            user_id = primary_key,
            data = str(json_text)
        ))


with concurrent.futures.ThreadPoolExecutor() as executor:
    while True:
        for i in os.listdir():
            if os.path.isfile(i) and (i != os.path.basename(__file__)) and (i not in FILE_NAMES):
                executor.submit(record_file, i)
                FILE_NAMES.append(i)

        sleep(0.001)