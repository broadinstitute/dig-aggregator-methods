#!/usr/bin/python3
from boto3.session import Session
import json
import os
import sqlalchemy


class PortalDB:
    def __init__(self):
        self.secret_id = os.environ['PORTAL_SECRET']
        self.db_name = os.environ['PORTAL_DB']
        self.config = None
        self.engine = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager')
            self.config = json.loads(client.get_secret_value(SecretId=self.secret_id)['SecretString'])
        return self.config

    def get_engine(self):
        if self.engine is None:
            self.config = self.get_config()
            self.engine = sqlalchemy.create_engine('{engine}://{username}:{password}@{host}:{port}/{db}'.format(
                engine=self.config['engine'] + ('+pymysql' if self.config['engine'] == 'mysql' else ''),
                username=self.config['username'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port'],
                db=self.db_name
            ))
        return self.engine

    def get_is_dichotomous(self, phenotype_name):
        with self.get_engine().connect() as connection:
            query = sqlalchemy.text(f'SELECT name, dichotomous FROM Phenotypes WHERE name = \"{phenotype_name}\"')
            rows = connection.execute(query).all()
            if len(rows) != 1:
                raise Exception(f"Invalid number of rows returned ({len(rows)}) for phenotype {phenotype_name}."
                                f"Check the database and try again.")
            if rows[0][1] is None:
                raise Exception(f"Invalid dichotomous information ({rows[0][1]}) for phenotype {phenotype_name}."
                                f"Check the database and try again.")
            return rows[0][1] == 1

    def get_dataset_data(self, dataset):
        with self.get_engine().connect() as connection:
            rows = connection.execute(
                sqlalchemy.text(f'SELECT name, ancestry FROM Datasets WHERE name = \"{dataset}\"')
            ).all()
        if len(rows) != 1:
            raise Exception(f"Impossible number of rows returned ({len(rows)}) for phenotype {dataset}. "
                            f"Check the database and try again.")
        if rows[0][0] is None or rows[0][1] is None:
            raise Exception(f"Invalid name / ancestry information ({rows[0][0]} / {rows[0][1]}) for dataset {dataset}. "
                            f"Check the database and try again.")
        return {'name': rows[0][0], 'ancestry': rows[0][1]}
