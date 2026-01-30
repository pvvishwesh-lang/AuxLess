import apache_beam as beam
import requests
import logging

class ReadFromAPI(beam.DoFn):
    def __init__(self,header,url):
        self.header=header
        self.url=url
    def process(self,element):
        res=requests.get(url=self.url,headers=self.header)
        if res.status_code==401:
            logging.info('The API is currently unresponsive. Please try again later...')
            return
        if not res.ok:
            res.raise_for_status()
            logging.error(res.text)
            return
        
        yield res.json()
        return
    
with beam.Pipeline() as p:
    headers={
        'Authorization':'Bearer user_1'
    }
    url='http://127.0.0.1:8000/v1/me/top/tracks'
    data=(
        p
        |'Seed'>>beam.Create([None])
        |'Read From API'>>beam.ParDo(ReadFromAPI(headers,url))
        |'Print'>>beam.Map(print)
    )


