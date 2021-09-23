import toml
from pydantic import BaseModel


class Database(BaseModel):
    host: str
    port: int
    user: str
    password: str
    dbname: str


class Elastic(BaseModel):
    host: str
    port: int


class Config(BaseModel):
    database: Database
    elastic: Elastic


with open('conf.toml') as file:
    dict_config = toml.load(file)

config = Config(**dict_config)
