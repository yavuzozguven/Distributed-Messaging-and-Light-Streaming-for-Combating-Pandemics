from pulsar import Function
import datetime
from paretoset import paretoset
import pandas as pd

class SqFunction(Function):
    def __init__(self):
        pass

    def write2_txt(self,fav,fws):
        with open("values.txt","a") as file_obj:
            file_obj.write(str(fav))
            file_obj.write(",")
            file_obj.write(str(fws))
            file_obj.write("\n")

    def clr_txt(self):
        open("values.txt","w").close()

    def paretoset(self):
        df = pd.read_csv("values.txt",sep=",")

        fav_count = df.iloc[:,0]
        fws_count = df.iloc[:,1]

        data = [fav_count,fws_count]

        headers = ["fav","follower"]


        tweets = pd.concat(data,axis=1,keys=headers)
        tweets['fav'] = pd.to_numeric(tweets['fav'])
        tweets['follower'] = pd.to_numeric(tweets['follower'])

        tw_p2set = pd.DataFrame({"fav":tweets['fav'],"follower":tweets['follower']})
        mask = paretoset(tw_p2set,sense=["max","min"])
        
        size = tw_p2set[mask].iloc[:,0].size
        sum_str = ""

        for i in range(0,size):
            fav = str(tw_p2set[mask].iloc[i,0])
            fws = str(tw_p2set[mask].iloc[i,1])
            sum_str = sum_str + fav + "," + fws + "\n"
        
        return sum_str

    def process(self, item, context):
        start_time = datetime.datetime.now()

        if str(item) == "start":
            self.clr_txt()

        if str(item) == "end":
            val = self.paretoset()
            end_time = datetime.datetime.now()
            time_diff = (end_time - start_time)
            execution_time = time_diff.total_seconds() * 1000

            full = str("end") + str(val) + str("!_!") + str(execution_time) + str(",") + str(datetime.datetime.now().time())
            return bytes(str(full),encoding="utf-8")

        inp = item.split("!|!")

        try:
            self.write2_txt(inp[0],inp[1])
        except:
            return

        end_time = datetime.datetime.now()
        time_diff = (end_time - start_time)
        execution_time = time_diff.total_seconds() * 1000

        full = str(inp[0]) + str(",") + str(inp[1]) + str("!_!") + str(execution_time) + str(",") + str(datetime.datetime.now().time())
        return bytes(full,encoding="utf-8")
