import platform,os,warnings
from flask import Flask
from gevent.pywsgi import WSGIServer
from apscheduler.schedulers.background import BackgroundScheduler
import to_cloud,changes2,config



warnings.filterwarnings("ignore", category=UserWarning)
# local_ip         = os.getenv('LOCAL_IP','192.168.10.9')
# server_port      = os.getenv('SERVER_PORT',3000)
# file_to_cloud= '12qKrKeMAj9uuo97sp6wQ3eutjBMug9zHSBkIeUaxryk'


delay = int(os.getenv("DELAY", 1))
app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    result = (""" <html> 
        <title>API server GRobot</title>
        <h4>API server GRobot</h4>
        <body><a href=/run>Run to Cloud </a></body>
    </html>
    """)
    return result

@app.route('/run', methods=['GET'])
def get_data():
    return to_cloud.to_cloud(config.file_to_cloud)

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(changes2.main_cycle, 'interval', seconds=delay * 60, id='from_cloud')
    scheduler.start()
    print(f"Running scheduler : {delay*60} seconds")

    if platform.system() == 'Windows':
        http_server = WSGIServer((config.local_ip, int(config.server_port)), app)
        print(f"Running HTTP-SERVER on port - http://" + config.local_ip + ':' + str(config.server_port))
    else:
        http_server = WSGIServer(('', int(config.server_port)), app)
        print(f"Running HTTP-SERVER on port :  {config.server_port}")
    http_server.serve_forever()