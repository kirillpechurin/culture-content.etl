import os

from dotenv import load_dotenv

load_dotenv("tests/.env.tests")

TEST_PG_HOST = os.getenv("TEST_PG_HOST")
TEST_PG_PORT = int(os.getenv("TEST_PG_PORT"))
TEST_PG_DB_NAME = os.getenv("TEST_PG_DB_NAME")
TEST_PG_USER = os.getenv("TEST_PG_USER")
TEST_PG_PASSWORD = os.getenv("TEST_PG_PASSWORD")

os.environ["EXTERNAL_PG_HOST"] = TEST_PG_HOST
os.environ["EXTERNAL_PG_PORT"] = str(TEST_PG_PORT)
os.environ["EXTERNAL_PG_DB_NAME"] = TEST_PG_DB_NAME
os.environ["EXTERNAL_PG_USER"] = TEST_PG_USER
os.environ["EXTERNAL_PG_PASSWORD"] = TEST_PG_PASSWORD
