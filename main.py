import pandas as pd
import numpy as np
import yfinance as yf
import datetime



etf_list=["DIA","IWD","IWF","IWM","COPX","SIL","SLX","USO","WOOD","SPY","QQQ", "IBB","XBI","XLB","XLC","XLE","XLF","XLI","XLK","XLP","XLRE","XLU","XLV","XLY","XME","XOP","XRT","XTN","GDX","BATT","BEDZ","BLOK","CLOU","EATZ","FTXL","GAMR","GBTC","IEZ","ITB","JETS","KARS","LIT","MJ","PBJ","PEJ","PRNT","QCLN","SOXX","TAN","VOX","XAR",]
#etf_list=["SPY"]

hoy= datetime.date.today()

symbol="USO"

def obtener_precios_etf(symbol, intervalo): #intervalo puede ser 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo

    data = pd.DataFrame(yf.download(symbol, end=hoy,interval = intervalo))
    data=data.copy()
    data["ETF"] = symbol
    data=data[["Open","High","Low","Close","ETF"]]
    data=data.reset_index()
    data["Year"]=data["Date"].dt.year
    data["Year"]=data["Year"].astype("str")
    data["Month"]=data["Date"].dt.month_name()
    data["Date day"]=data["Date"].dt.day
    data["Date day"]=data["Date day"].astype("str")
    data["Week Number"]=data["Date"].dt.isocalendar().week
    data["Week Number"]="W"+ data["Week Number"].astype("str")
    data["Day Name"]=data["Date"].dt.day_name()
    data['Close % Change'] = 0
    data['Close_prev'] = data['Close'].shift(1)
    data['Close % Change'] = (data['Close'] - data['Close_prev']) / data['Close_prev'] * 100
    data['Close % Change'] = data['Close % Change'].round(2)
    data["Change Type"] = data["Close % Change"].apply(lambda x: get_change_type(x))
    data['Open'] = data['Open'].round(2)
    data['High'] = data['High'].round(2)
    data['Low'] = data['Low'].round(2)
    data['Close'] = data['Close'].round(2)
    data = data.drop('Close_prev', axis=1)
    return data

        


def get_change_type(change):
    if change >= 0:
        return "Pos"
    else:
        return "Neg"



def obtenerDataframeVariosSymbols(lista_symbols,temporalidad):
    dataframeColumns = pd.DataFrame(columns=[ "Date","Open","High","Low","Close","ETF","Year","Month","Date day","Week Number","Day Name","Close % Change","Change Type"])
    for ETF in lista_symbols:
        Newdata=obtener_precios_etf(ETF,temporalidad)
        dataframeColumns = pd.concat([Newdata, dataframeColumns])
    return dataframeColumns
        

def guardarCSV(nombrearchivo,dataframe):
    rutaguardado="./CSV_Files/"+nombrearchivo
    dataframe.to_csv(rutaguardado, sep=";",decimal=',',index=False)
    return

def descargarHistoricos(listasymbols,temporalidad,nombreArchivo):
    data=obtenerDataframeVariosSymbols(listasymbols,temporalidad)
    guardarCSV(nombreArchivo,data)
    return data




data1=descargarHistoricos(etf_list,"1wk","dataWeekly.csv")
print(data1.head())
data2=descargarHistoricos(etf_list,"1mo","dataMonthly.csv")
print(data2.head())
data3=descargarHistoricos(etf_list,"1d","dataDaily.csv")
print(data3.head())

actualizanndog