import os
import sys
import sqlalchemy as sa
from dotenv import load_dotenv
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from abc import abstractmethod

load_dotenv()

ENDPOINT = os.getenv("ENDPOINT")
ENDPOINT_READER = os.getenv("ENDPOINT_READER")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")
PASSWORD = os.getenv("PASSWORD")
DBUSER = os.getenv("DBUSER")

class SingletonDataBase: 
    """
    Initiate a connection to our Postgres DB once 
    """
    _instance = None

    def __new__(cls): 
        if cls._instance is None:
            cls._instance = super(SingletonDataBase, cls).__new__(cls)
        return cls._instance

    def __init__(self):
            self.engine = sa.create_engine(f"postgresql+psycopg2://{DBUSER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DBNAME}")
            self.base = automap_base()
            self.base.prepare(self.engine, reflect=True)
            self.session = Session(self.engine)


class BaseDB():
    """
    Abstract class for database tables
    """
    @abstractmethod
    def __init__(self, table):
        self.db = SingletonDataBase()
        self.table = getattr(self.db.base.classes, table)
            
class CNFT(BaseDB): 
    def __init__(self):
         super().__init__("cNft")
       
class imageOCRTable(BaseDB):
    def __init__(self):
         super().__init__("imageOCRTable")

class jsonMetadataTable(BaseDB):
    def __init__(self):
         super().__init__("jsonMetadataTable")

class treeTable(BaseDB):
    def __init__(self):
         super().__init__("treeTable")


