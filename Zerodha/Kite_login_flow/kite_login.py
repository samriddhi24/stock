from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException
import psycopg2, configparser
config = configparser.ConfigParser()
config.read('config.ini')


class Login:
    def __init__(self, api_key=None):
        if api_key==None:
            self.api_key = config.get("CREDS", "api_key")
        else:
            self.api_key = api_key
        self.kite = KiteConnect(api_key=self.api_key)
        self.access_token = self.get_access_token()

    def get_access_token(self):
        try:
            connection = psycopg2.connect(
                dbname=config.get("DEVDB", "database"),
                user=config.get("DEVDB", "username"),
                password="C=rz6#HO8D[dsXyYA23SVR%&*#OfkycQIl",
                host=config.get("DEVDB", "host"),
                port=config.get("DEVDB", "port"),
            )
            cursor = connection.cursor()

            # Execute a query to retrieve the request token
            cursor.execute("SELECT access_token FROM broker_tokens where user_id = 'IYQ877' and broker = 'Zerodha'")
            result = cursor.fetchone()
            # Fetch the token from the query result
            if result:
                return result[0]
            else:
                raise Exception("Request token not found for the given user_id")

        finally:
            # Close the cursor and the connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def login(self):
        try:
            self.kite.set_access_token(self.access_token)
            return True
        except TokenException as e:
            print(e)
            print("Error class:", type(e).__name__)
            print("Token exception occurred. Please check the request token.")
            return False
        except Exception as e:
            print(e)
            print("Error class:", type(e).__name__)
            print("An error occurred while generating the session.")
            return False

    def get_kite_connect(self):
        self.login()
        return self.kite



