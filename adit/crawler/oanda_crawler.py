#https://medium.com/@dodgervl/get-free-financial-historical-data-from-oanda-v20-api-in-3-easy-steps-54a485cebcce
from oandapyV20 import API
import oandapyV20.endpoints.instruments as instruments
import pandas as pd
import datetime
from dateutil import parser
from config.const import OANDA_TOKEN

access_token=OANDA_TOKEN
client=API(access_token)

step=21600 # equals to 6h in UNIX_time.Depends on granulariy.
# for 5s 6 hours is maximum granularity time.
# for 1m 21600*12 for 5m 21600*12*5.
granularity="S5"
begin_unix=parser.parse("2000-01-01").timestamp()
end_unix=parser.parse("2000-01-03").timestamp()

i=begin_unix+step
dataset=pd.DataFrame()
params={"from": str(i-step),
        "to": str(i),
        "granularity":granularity,
        "price":'A' } # 'A' stands for ask price;
                      # if you want to get Bid use 'B' instead or 'AB' for both.
while i<=end_unix:
    params['from']=str(i-step)
    params['to']=str(i)
    r=instruments.InstrumentsCandles(instrument="EUR_USD",params=params)
    data = client.request(r)
    results= [{"time":x['time'],"open":float(x['ask']['o']),"high":float(x['ask']['h']),
              "low":float(x['ask']['l']),"close":float(x['ask']['c']),"volume":float(x['volume'])} for x in data['candles']]
    df = pd.DataFrame(results)
    if dataset.empty: dataset=df.copy()
    else: dataset=dataset.append(df, ignore_index=True)
    if(i+step)>=end_unix:
        params['from']=str(i)
        params['to']=str(end_unix)
        r=instruments.InstrumentsCandles(instrument="EUR_USD",params=params)
        data = client.request(r)
        results= [{"time":x['time'],"open":float(x['ask']['o']),"high":float(x['ask']['h']),
                  "low":float(x['ask']['l']),"close":float(x['ask']['c'])} for x in data['candles']]
        df = pd.DataFrame(results)
        i+=step
        dataset=dataset.append(df, ignore_index=True)
    if len(dataset)>2000000:
        dataset.to_csv("EURUSD"+"_"+granularity+"_"+dataset['time'][0].split('T')[0]+"_"+dataset['time'][len(dataset)-1].split('T')[0]+'.csv',index=False)
        dataset=pd.DataFrame()
    i+=step
dataset.to_csv("EURUSD"+"_"+granularity+"_"+dataset['time'][0].split('T')[0]+"_"+dataset['time'][len(dataset)-1].split('T')[0]+'.csv',index=False)
