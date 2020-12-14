from pulsar import Function
import datetime
import os
from google.cloud import translate_v2 as translate

class TranslateFunction(Function):
    def __init__(self):
        self.other_topic = "persistent://public/default/tr_other"
        self.USA_topic = "persistent://public/default/tr_USA"
        self.Turkey_topic = "persistent://public/default/tr_Turkey"
        self.UK_topic = "persistent://public/default/tr_UK"
        self.France_topic = "persistent://public/default/tr_France"

    def calc_run(self,start_time,translate_time):
        end_time = datetime.datetime.now()
        time_diff = (end_time - start_time)
        execution_time = time_diff.total_seconds() * 1000

        without_translate = execution_time - translate_time

        full_text = str("!_!") + str(execution_time) + str(",") + str(translate_time) + str(",") + str(without_translate)
        full_text = full_text + str(",") + str(datetime.datetime.now().time())
        
        return bytes(full_text,encoding="utf-8")

    def translate(self,text):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"/home/yavuzozguvens/ap/cloudkey.json"

        translate_client = translate.Client()

        inp = text
        target = 'tr'

        output = translate_client.translate(
        inp,
        target_language=target
        )

        return output['translatedText']

    def process(self, item, context):
        start_time = datetime.datetime.now()

        inp = item.split("!|!")

        location = inp[0]
        coordinates = inp[0].split(",")
        longitude = float(coordinates[0])
        try:
            latitude = float(coordinates[1])
        except:
            return

        lat_long = str(latitude) + str(",") + str(longitude)
        text = str(inp[1])

        translate_start = datetime.datetime.now()
        translated = self.translate(text)
        translate_end = datetime.datetime.now()
        time_diff = (translate_end - translate_start)
        execution_time = time_diff.total_seconds() * 1000

        runt = self.calc_run(start_time,execution_time) 


        full = str(translated) + str(" Lat,Long:") + lat_long
        full = bytes(full,encoding="utf-8")

        if(31 < latitude < 48) and (-126 < longitude <-67):
            context.publish(self.USA_topic,full+runt)
            return
        
        if(36 < latitude < 41.9) and (26 < longitude < 41):
            context.publish(self.Turkey_topic,full+runt)
            return
        
        if(43 < latitude < 49.7) and (-1 < longitude < 7):
            context.publish(self.France_topic,full+runt)
            return

        if(50.17 < latitude < 58.9) and (-5.6 < longitude < 1.79):
            context.publish(self.UK_topic,full+runt)
            return

        else:
            context.publish(self.other_topic,full+runt)
