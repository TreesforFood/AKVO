from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()


class Config:
    def __init__(self):

        self.CONF = {
            "CLIENT_ID": os.getenv("CLIENT_ID"),
            "USERNAME" : os.getenv("USERNAME"),
            "PASSWORD": os.getenv("PASSWORD"),
            "GRANT_TYPE": os.getenv("GRANT_TYPE"),
            "SCOPE": os.getenv("SCOPE"),
            "DATABASE_URL" : os.getenv("DATABASE_URL"),
            "DATABASE_PSTGRS" : os.getenv("DATABASE_PSTGRS"),
            "USER_PSTGRS" : os.getenv("USER_PSTGRS"),
            "PASSWORD_PSTGRS" : os.getenv("PASSWORD_PSTGRS"),
        }

    def val_config(self):

        for c in self.data.keys():
            if self.data[c] == "":
                return False

        return True
