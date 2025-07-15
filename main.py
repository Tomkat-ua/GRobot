import platform,os,changes2
from flask import Flask,request
from gevent.pywsgi import WSGIServer
from apscheduler.schedulers.background import BackgroundScheduler

import to_cloud

local_ip         = os.getenv('LOCAL_IP','192.168.10.9')
server_port      = os.getenv('SERVER_PORT',3000)

delay = int(os.getenv("DELAY", 1))
app = Flask(__name__)

@app.route('/run', methods=['GET'])
def get_data():
    return to_cloud.to_cloud()

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(changes2.main_cycle, 'interval', seconds=delay * 60, id='from_cloud')
    scheduler.start()
    print(f"Running scheduler : {delay*60} seconds")

    if platform.system() == 'Windows':
        http_server = WSGIServer((local_ip, int(server_port)), app)
        print(f"Running HTTP-SERVER on port - http://" + local_ip + ':' + str(server_port))
    else:
        http_server = WSGIServer(('', int(server_port)), app)
        print(f"Running HTTP-SERVER on port :  {server_port}")
    http_server.serve_forever()