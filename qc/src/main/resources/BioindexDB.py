from boto3.session import Session
import json
import sqlalchemy


class BioIndexDB:
    def __init__(self):
        self.secret_id = 'dig-bio-portal'
        self.config = None
        self.engine = None

    def get_config(self):
        if self.config is None:
            client = Session().client('secretsmanager')
            self.config = json.loads(client.get_secret_value(SecretId='dig-bio-portal')['SecretString'])
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
                db=self.config['dbname']
            ))
        return self.engine

    def get_is_dichotomous(self, phenotype_name):
        with self.get_engine().connect() as connection:
            rows = connection.execute(f'SELECT name, dichotomous FROM Phenotypes WHERE name = \"{phenotype_name}\"').all()
            if len(rows) != 1:
                raise Exception(f"Impossible number of rows returned ({len(rows)}) for phenotype {phenotype_name}."
                                f"Check the database and try again.")
            return rows[0][1] == 1
