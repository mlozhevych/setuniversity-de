import inspect
import os

from dotenv import load_dotenv
from flask import Flask, request
from flask_smorest import Api, Blueprint

from analyze_ads_rest_api.db import db
from analyze_ads_rest_api.resources import blueprints

server = Flask(__name__)
load_dotenv()

# TTLs (seconds)
TTL_CAMPAIGN = os.getenv('TTL_CAMPAIGN', 30)  # 30 seconds
TTL_ADVERTISER = os.getenv('TTL_ADVERTISER', 300)  # 5 minutes
TTL_USER = os.getenv('TTL_USER', 60)  # 1 minute

DATABASE_USER = os.getenv('DATABASE_USER', 'adtech')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD', 'adtechpass')
DATABASE_HOST = os.getenv('DATABASE_HOST', 'localhost')
DATABASE_DB = os.getenv('DATABASE_DB', 'AdTech')

server.config[
    'SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}/{DATABASE_DB}"
server.config["API_TITLE"] = "REST API for library management"
server.config["API_VERSION"] = "v1"
server.config["OPENAPI_VERSION"] = "3.0.3"
server.config["OPENAPI_URL_PREFIX"] = "/"
server.config["OPENAPI_SWAGGER_UI_PATH"] = "/swagger-ui"
server.config["OPENAPI_SWAGGER_UI_URL"] = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"
server.config['DEBUG'] = True

db.init_app(server)
api = Api(server)


@server.before_request
def log_request_data():
    print("Headers:", request.headers)
    print("Body:", request.get_data(as_text=True))
    print("Content-Type:", request.content_type)
    print("JSON:", request.get_json(silent=True))


with server.app_context():
    db.create_all()

adtech = Blueprint("adtech", "adtech", url_prefix="/adtech", description="AdTech REST API")

blueprints_list = [
    bp for name, bp in inspect.getmembers(blueprints)
    if isinstance(bp, Blueprint)
]

for bp in blueprints_list:
    adtech.register_blueprint(bp)

api.register_blueprint(adtech)

if __name__ == "__main__":
    server.run(host="0.0.0.0", port=5002)
