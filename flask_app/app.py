from flask import Flask, request
from celery import Celery
import requests, os,  base64
from io import BytesIO
from pytube import YouTube 

app = Flask(__name__)
simple_app = Celery(
    'simple_worker', broker='redis://redis:6379/0', backend='redis://redis:6379/0')


@app.route('/simple_start_task')
def call_method():
    app.logger.info("Invoking Method ")

    link = request.args.get('link')
    email = request.args.get('email')
    unique_id = request.args.get('Unique id')
    
    if 'amazonaws' in link: 
        yt_title = link.split('/')[-1].replace('%', '_')

    else: 
        yt_obj = YouTube(link)
        yt_title = yt_obj.title  

    r = simple_app.send_task('tasks.predict', kwargs={'link': link, 'email': email, 'youtube_title': yt_title, 'unique_id':unique_id })

    global link_yt 
    link_yt = link 

    global email_
    email_ = email

    app.logger.info(r.backend)

    global id_ 
    id = r.id 
    id_ = id 
    return id 

@app.route('/simple_task_status', methods=['POST'])
def get_status():
    task_id = request.args.get('task_id') 
    status = simple_app.AsyncResult(task_id, app=simple_app)

    
    while True: 
        if str(status.state) == 'SUCCESS':
            result = simple_app.AsyncResult(task_id).result 

            with open("result.txt", 'w') as f:
                f.write(str(result))
            with open("result.txt", 'rb') as file:
                 mp3bytes = BytesIO(file.read())

            mp3 = base64.b64encode(mp3bytes.getvalue()).decode("ISO-8859-1")

            payload={'youtube_link': link_yt, 'file': mp3, 'id' :id_, 'email': email_}  
            files = [(file, mp3bytes)]
            response = requests.request("POST", url, headers=headers, data=payload)
            os.remove("result.txt")
            # return response.text 
            break 


@app.route('/simple_task_result')
def task_result():
    task_id = request.args.get('task_id')
    result = simple_app.AsyncResult(task_id).result
    return str(result)

