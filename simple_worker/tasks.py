from celery import Celery
import banana_dev as banana
from datetime import timedelta
import requests, base64, urllib, creds, os
from celery.utils.log import get_task_logger

# logging and celery 
logger = get_task_logger(__name__)
app = Celery('tasks', broker='redis://redis:6379/0',
             backend='redis://redis:6379/0')

from dotenv import load_dotenv 

def configure(): 
    load_dotenv()

configure()
# api and model key 
# api_key = creds.api_key
# model_key = creds.model_key

# trying with new key! 
api_key = os.getenv('api_key')
model_key = os.getenv('model_key')


def create_subtitle(data:dict) -> str:
    """ Takes the input as banana output and convert to youtube format"""
    data = data['modelOutputs'][0]

    all = ""
    for idx in range(len(data['segments'])):
        start = str(timedelta(seconds=data['segments'][idx]['start']))
        end = str(timedelta(seconds=data['segments'][idx]['end']))
        text = data['segments'][idx]['text']
        final =str(idx+1)+'\n'+start+' --> '+end+'\n'+text+'\n\n'
        all += final

    return all


def shorten(url_long:str) -> str:
    URL = "http://tinyurl.com/api-create.php"
    url = URL + "?" + urllib.parse.urlencode({"url": url_long})
    res = requests.get(url) 

    return res.text

# backend worker 
@app.task()
def predict(link:str, email:str, youtube_title:str, unique_id):


    url = f"{os.getenv('url')}{unique_id}"
    headers = {'Authorization': os.getenv('auth')}
  
    logger.info('Got Request - Starting work ')

    try: 
        if 'amazonaws' in link: 
            link = shorten(f'https:{link}')
        
        model_payload = {'link':link}
        logger.info(f"Model Payload: {model_payload}") 
        logger.info("Sent the paytload to banana!")
        
        out = banana.run(api_key, model_key, model_payload)
        logger.info("Got the output from banana") 
        
        out = create_subtitle(out)

        logger.info("The output is created and it's preparing to send to bubble io!")
        mp3 = base64.b64encode(bytes(str(out), 'utf-8'))
        payload={'file': mp3, 'Email': email, 'youtube_title': youtube_title, 'status':'Success'}  

        logger.info("Payload is Ready! ")
        response = requests.request("PATCH", url, headers=headers, data=payload)

        logger.info("Succesfully sent the file to bubble! Check in bubble")
        logger.info('Work Finished ')

        return out 
    
    except Exception as e: 
        payload = {'file': 'RXJyb3IgaW4gZmlsZSEg', 'Email': email, 'youtube_title':youtube_title, 'status': 'Failed'}
        logger.info(f"File is not processed there are some error: {e}")
        response = requests.request("PATCH", url, headers=headers, data=payload)
        return str(e)


"""
# PREVIOUS API's

# response = requests.get(
#     f'http://quickzam.pythonanywhere.com/give_bytes?link=https://www.youtube.com/watch?v={link}') ## python anywhere

# response = requests.get(
#     f"https://lionfish-app-wynde.ondigitalocean.app/give_bytes?link={link}") # digital Ocean flask app

"""
