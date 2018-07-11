# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 14:32:55 2017

@author: Antoine
"""

import os
import requests
import pandas as pd
import numpy as np
import time

#==========================================================================
#   CANDEL-MAKER --- GUIDE
#==========================================================================

#   -> définir un fichier où les données seront enregistrées (modifier "racine") (I.)
#   -> choisir la pair de crypto-monnaie (choisir la valeur de i)  (II.)
#   -> Téléchargement des données transactions via API (III.)
#   -> Mettre sous forme de chandelles (IV.)


#%%========================================================================
#   I. STOCKAGE DATA
#==========================================================================

# Datafile's root
racines = {
        'C:\\Users\\antoine\\Desktop\\Polytechnique\\Binet\\X Finance\\Cryptocurrencies exchange rate' :
            'C:/Users/antoine/Desktop/Polytechnique/Binet/X Finance/Cryptocurrencies exchange rate/Data/Ignored Data/'
           }

racine = racines[os.getcwd()]

#%%========================================================================
#   II. CHOIX PAIR CRYPTO
#==========================================================================

i = 0
pairs = ['XXBTZEUR', 'XETHZEUR', 'XLTCZEUR', 'BCHEUR', 'DASHEUR']
pair = pairs[i]


#%%========================================================================
#   III. TELECHARGEMENT TRANSACTION KRAKEN API
#==========================================================================

#Fonction principale : chargement des opérations sur Kraken via l'API

    #   f permet de télécharger les données depuis le début de l'historique des transactions (ou de reprendre depuis le moment où vous vous êtes arrêtés).
    #   Les données brutes sont ajoutées une à une sur un pandasFrame  et sont mises en forme et enregistées sur un autre pandasFrame à intervelles réguliers.
    
    #@ PARAMETRE  j: fixe le nombre de jours de données que vous voulez télécharger entre chaque sauvegarde
    
    #Au cours du processus, il ne FAUT PAS fermer python si le dernier message affiché est "you cannot interrupt"... Il faut attendre l'apparition de "Now you can"
    
def f(j=15):
    
    #paramètres
    current=time.time()
    
    #charger données déjà existantes
    try:
        dataset = pd.read_csv(open(racine + 'Data_' + pair + '/New_Data_' + pair + '.csv'), index_col=0)
    except Exception:
        dataset = pd.DataFrame()
    
    #charge l'id du dernier paquet téléchargé à partir du fichier "last_id"
    try :
        file = open(racine + 'Data_' + pair + '/last_id_' + pair + '.txt', "r") 
        last_id = file.readlines()[0]
        file.close()
    except Exception:
        last_id = '0'
        
    n = 0
    
    #Chargement des données jusqu'aujourd'hui
    while(int(last_id)/10**9 < current):
        data = pd.DataFrame()
        dif = 0
        while(dif < 24*3600*10**9*j):
            dif -= int(last_id)
            url = 'https://api.kraken.com/0/public/Trades?pair=' + pair + '&since='+last_id
            (last_id, data) = get_answer(url, data)
            print(last_id)
            dif += int(last_id)
        n += 1
        print('{} savings'.format(n))
        print("\nyou cannot interrupt")
        time.sleep(5)
        data.columns = ['price', 'volume', 'time', 'buy/sell', 'market/limit', 'miscellaneous']
        dataset = dataset.append(data, ignore_index=True)
        dataset.to_csv(racine + 'Data_' + pair + '/New_Data_' + pair + '.csv')
        file = open(racine + 'Data_' + pair + '/last_id_' + pair + '.txt', "w")
        file.write(last_id)
        file.close()
        print("now you can\n")
    print("The end")


# Fonction auxilliaire de f

def get_answer(url, data):
    new_data = requests.get(url)
    
    try:
        last_id = new_data.json()['result']['last']
    except Exception:
        return get_answer(url,data)
    
    data = data.append(new_data.json()['result'][pair], ignore_index=True)
    return (last_id,data)

#===================================================================
#   A utiliser lorsque le ficher newDataXXX/ZEUR devient trop lourd
    #   Transfert des données de newDataXXX/ZEUR vers DataXXX/ZEUR

def transfert():
    
    try:
        dataset = pd.read_csv(open(racine + 'Data_' + pair + '/Data_' + pair + '.csv'), index_col=0)
    except Exception:
        dataset = pd.DataFrame()
    

    data_add = pd.read_csv(open(racine + 'Data_' + pair + '/New_Data_' + pair + '.csv'), index_col=0)
    
    dataset = dataset.append(form(data_add,dataset.shape[0]), ignore_index=True)
    
    data_add = pd.DataFrame(columns=['price', 'volume', 'volume_EUR', 'time', 'buy/sell', 'market/limit', 'miscellaneous'])
    data_add.to_csv(racine + 'Data_' + pair + '/New_Data_' + pair + '.csv')
    
    dataset.to_csv(racine + 'Data_' + pair + '/Data_' + pair + '.csv')
    
    file = open(racine + 'Data_' + pair + '/last_id_' + pair + '.txt', "r") 
    last_id = file.readlines()[0]
    file.close()
    
    file = open(racine + 'Data_' + pair + '/previous_last_id_' + pair + '.txt', "w")
    file.write(last_id)
    file.close()


#   Fonction auxilliaire de transfert

def form(dataset, decalage=0):#met les données au format ['time',  'price',  'volume',  'buy',  'market',  'candle_24h',  'candle_4h',  'candle_1h',  'candle_15min',  'candle_1min',  'batch_id'] en vue de former les chandelles
    data = pd.DataFrame(columns=['time', 'price', 'volume', 'volume_EUR', 'buy', 'market', 'batch_id'])
    
    data[['time',  'price',  'volume',  'buy',  'market']] = dataset[['time', 'price', 'volume', 'buy/sell', 'market/limit']]
    data['volume_EUR'] = data['volume'] * data['price']
    data['batch_id'] = data.index.values
    data['batch_id'] = data['batch_id'].apply(lambda x: int((x+decalage) / 1000))

    return data
    
    
#%%========================================================================
#   IV. FORMATAGE -- CHANDELLES
#==========================================================================

#   Creation des chandelles (dans le cas d'une première création de chandelles)

def candle(x=24, u='h'):#sélection de la chandelle. x : entier; u: unité [seconde:s,minute:m,heure:h,jour:j]

    dataset = pd.read_csv(open(racine + 'Data_' + pair + '/Data_' + pair + '.csv'), index_col=0) #import fichier csv brut
    
    dic = {'s': 1, 'm': 60, 'h': 3600, 'j': 3600*24}
    m = x * dic[u]
    can = 'candle_' + str(x) + u

    newCandle = pd.DataFrame(columns=['nb', 'V', 'V_EUR',  'Pmin',  'PQ1',  'Pmed',  'PQ3',  'Pmax',  'P_open',  'P_close',  'return']) # nb : nombre d'opération, V:volume echange en crypto , Pmin,PQ1,Pmed,PQ3,Pmax:min, premier quartile, médiane, troisième quartile, max des prix des échanges d'ordre 'market', 'return':(P_close-P_open)/P_open, P_open : premier prix de la chandelle
    
    
    df = dataset[['time',  'volume',  'volume_EUR',  'price']] #crée un dataFrame à partir des collonees 'time',  'volume',  'volume_EUR', 'price'
    
    
    d_group = df.groupby(df['time']//m)

    #Remplissage des colonnes
    
    newCandle['nb'] = d_group['volume'].count()
    newCandle['V'] = d_group['volume'].apply(lambda x: x.sum())
    newCandle['V_EUR'] = d_group['volume_EUR'].apply(lambda x: x.sum())
    newCandle['Pmin'] = d_group['price'].apply(lambda x: x.min())
    newCandle['PQ1'] = d_group['price'].apply(lambda x: x.quantile(0.25))
    newCandle['Pmed'] = d_group['price'].apply(lambda x: x.quantile(0.5))
    newCandle['PQ3'] = d_group['price'].apply(lambda x: x.quantile(0.75))
    newCandle['Pmax'] = d_group['price'].apply(lambda x: x.max())
    newCandle['P_open'] = d_group['price'].apply(lambda x: x.iloc[0])
    newCandle['P_close'] = d_group['price'].apply(lambda x: x.iloc[-1])
    newCandle['return'] = (newCandle['P_close']-newCandle['P_open'])/newCandle['P_open']
    
    newCandle = newCandle.drop(newCandle.index[-1]) #on supprime la dernière ligne qui est probablement incomplète
    
    newCandle.index = [int(x) for x in newCandle.index]
    newCandle.index.name = can
    
    #enregistrement du nouveau fichier
    newCandle.to_csv(racine + 'Data_' + pair + '/' + pair + '_' + can + '.csv')


#   Update des chandelles (à partir d'un fichier préexistant)

def completeCandle(x=24, u='h'):#Complete un document contenant les chandelles déjà téléchargées. 
    dataset = pd.read_csv(open(racine + 'Data_' + pair + '/Data_' + pair + '.csv'), index_col=0) #import fichier csv brut
    
    dic = {'s':1,'m':60,'h':3600,'j':3600*24}
    m = x*dic[u]
    can = 'candle_' + str(x) + u

    chandelles = pd.read_csv(open(racine + 'Data_' + pair + '/' + pair + '_'+can+'.csv'), index_col=0) #import du fichier contenant les chandelles déjà enregistrées
    df = dataset[dataset.time>=m*(chandelles.index[-1]+1)][['time',  'volume',  'volume_EUR',  'price']] #crée un dataFrame à partir des collonees 'time',  'volume', 'market', 'price'
    
    newCandle = pd.DataFrame(columns=['nb',  'V',  'V_EUR',  'Pmin',  'PQ1',  'Pmed',  'PQ3',  'Pmax',  'P_open',  'P_close',  'return'])
    
    d_group = df.groupby(df['time']//m)

    #Remplissage des colonnes
    
    newCandle['nb'] = d_group['volume'].count()
    newCandle['V'] = d_group['volume'].apply(lambda x: x.sum())
    newCandle['V_EUR'] = d_group['volume_EUR'].apply(lambda x: x.sum())
    newCandle['Pmin'] = d_group['price'].apply(lambda x: x.min())
    newCandle['PQ1'] = d_group['price'].apply(lambda x: x.quantile(0.25))
    newCandle['Pmed'] = d_group['price'].apply(lambda x: x.quantile(0.5))
    newCandle['PQ3'] = d_group['price'].apply(lambda x: x.quantile(0.75))
    newCandle['Pmax'] = d_group['price'].apply(lambda x: x.max())
    newCandle['P_open'] = d_group['price'].apply(lambda x: x.iloc[0])
    newCandle['P_close'] = d_group['price'].apply(lambda x: x.iloc[-1])
    newCandle['return'] =(newCandle['P_close']-newCandle['P_open'])/newCandle['P_open']
    
    newCandle = newCandle.drop(newCandle.index[-1]) #on supprime la dernière ligne qui est probablement incomplète
    newCandle.index = [int(x) for x in newCandle.index]
    newCandle.index.name = can
    
    chandelles = chandelles.append(newCandle, ignore_index=False)
    #enregistrement du nouveau fichier
    chandelles.to_csv(racine + 'Data_' + pair + '/' + pair + '_' + can + '.csv')

#===================================================================

#   Creation des chandelles PLEINES (même s'il n'y a pas de transaction, il y a une ligne dans le csv) (dans le cas d'une première création de chandelles)
#   A utiliser après avoir créé les chandelles standards

def filledCandles(x=24, u='h'):
    
    can = 'candle_'+str(x)+u
    
    dataset = pd.read_csv(open(racine + 'Data_' + pair + '/' + pair + '_'+can+'.csv'), index_col=0)
    
    data = dataset.reindex(np.arange(dataset.index[0],dataset.index[-1]+1))
    
    for j in range(dataset.index[0],dataset.index[-1]):
        if(np.isnan(data.loc[j][0])):
            data.loc[j]['nb':'V'] = 0
            data.loc[j]['Pmin':'P_close'] = data.loc[j-1]['P_close']
            data.loc[j]['return'] = 0
    
    data = data.drop(data.index[-1])
    
    data.to_csv(racine + 'Data_' + pair + '/' + pair + '_complete_' + can + '.csv')


#   Update des chandelles PLEINES (même s'il n'y a pas de transaction, il y a une ligne dans le csv) (à partir d'un fichier préexistant)    
#   A utiliser après avoir créé les chandelles standards

def completeFilledCandles(x=24,u='h'):
    
    can = 'candle_' + str(x) + u
    
    newData = pd.read_csv(open(racine + 'Data_' + pair + '/' + pair + '_' + can + '.csv'), index_col=0)
    
    data = pd.read_csv(open(racine + 'Data_' + pair + '/' + pair + '_complete_' + can + '.csv'), index_col=0)
    
    newData = newData[newData.index>data.index[-1] - 1]
    newData = newData.reindex(np.arange(data.index[-1],newData.index[-1]+1))
    
    for j in range(newData.index[1],newData.index[-1]):
        if(np.isnan(newData.loc[j][0])):
            newData.loc[j]['nb':'V'] = 0
            newData.loc[j]['Pmin':'P_close'] = newData.loc[j-1]['P_close']
            newData.loc[j]['return'] = 0
        
    newData = newData.drop(newData.index[0])
    newData = newData.drop(newData.index[-1])
    
    data = data.append(newData, ignore_index=False)
    
    data.to_csv(racine + 'Data_' + pair + '/' + pair + '_complete_' + can + '.csv')

#===================================================================

def totalUpdate(x=24,u='h'):
    transfert()
    completeCandle(x,u)
    completeFilledCandles(x,u)
    