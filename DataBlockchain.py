import requests
import pandas as pd
import time
import os

# Datafile's root
racines = {
        'C:\\Users\\antoine\\Desktop\\Polytechnique\\Binet\\X Finance\\Cryptocurrencies exchange rate' :
            'C:/Users/antoine/Desktop/Polytechnique/Binet/X Finance/Cryptocurrencies exchange rate/Data/Shared Data/Data_Blockchain/'
           }

racine = racines[os.getcwd()]

def get_answer(url):
    try:
        new_data = requests.get(url).json()["blocks"]
    except Exception:
        print('fail')
        return get_answer(url)
    return (new_data[0])

def getV(tx):
    v = 0
    for trans in tx:
        for dic in trans['out']:
            v += dic["value"]
    return v


def f(j=100):
    
    #paramètres
    current_height = requests.get('https://blockchain.info/fr/q/getblockcount').json() #récupérer la hauteur du bloc le plus récent
    
    #charger données déjà existantes
    dataset = pd.read_csv(open(racine + 'New_Data_Blockchain.csv'), index_col=0)
    
    try :
        file = open(racine + 'height.txt', "r") 
        height = file.readlines()[0]  #hauteur du dernier block pris en compte +1
        file.close()
    except Exception:
        height = '214563' #hauteur du premier block de 2013
        
    n = 0
    
    while(int(height) < current_height-j):
        data = pd.DataFrame()
        nBlock = 0
        while(nBlock < j):
            nBlock += 1
            url = 'https://blockchain.info/fr/block-height/' + str(height) + '?format=json'
            answer = get_answer(url)
            height = answer['height'] #hauteur du block
            t = answer['time']    #temps auquel le block a été émis
            nbTrans = answer['n_tx']  #nb de transaction contenue dans le block
            V = getV(answer['tx'])/10**8 #nombre de BTC tranmis (out)
            fee = answer['fee']/10**8
            data = data.append([[t,nbTrans,V,fee,height]], ignore_index=True)
            print(height)
            height += 1
        n += 1
        print('\n{} savings'.format(n))
        print("\nyou cannot interrupt")
        time.sleep(5)
        data.columns = ['time', 'nbTrans', 'V', 'Fee','height']
        dataset = dataset.append(data,ignore_index=True)
        dataset.to_csv(racine + 'New_Data_Blockchain.csv')
        file = open(racine + 'height.txt', "w")
        file.write(str(height))
        file.close()
        print("now you can\n")
    print("The end")
    

def transfert():#transfert des données chargées depuis un certain temps vers le fichier contenant toutes les opérations
    
    dataset = pd.read_csv(open(racine + 'Data_Blockchain.csv'), index_col=0)
    data_add = pd.read_csv(open(racine + 'New_Data_Blockchain.csv'), index_col=0)
    
    dataset=dataset.append(form(data_add), ignore_index=True)
    dataset.to_csv(racine + 'Data_Blockchain.csv')
    
    new_data = pd.DataFrame(columns=['time', 'nbTrans', 'V', 'Fee','height'])
    new_data.to_csv(racine + 'New_Data_Blockchain.csv')
    
    
    
    
    file = open(racine + '/height.txt', "r") 
    height = file.readlines()[0]
    file.close()
    
    file = open(racine + '/previous_height.txt',"w")
    file.write(height)
    file.close()
    
def form(dataset):#met les données au format ['time','nbTrans','volume','fee','candle_24h','candle_4h','candle_1h','height'] en vue de former les chandelles
    data = pd.DataFrame(columns=['time','nbTrans','volume','fee','height'])
    
    data[['time','nbTrans','volume','fee']] = dataset[['time', 'nbTrans', 'V', 'Fee']]
    data['candle_24h'] = data['time'].apply(lambda x: int(x/(24*3600)))
    data['candle_4h'] = data['time'].apply(lambda x: int(x/(4*3600)))
    data['candle_1h'] = data['time'].apply(lambda x: int(x/(3600)))
    data['height'] = dataset['height']

    return data
    
def candle(x=24, u='h'):#x:sélection de la chandelle : u->unité de temps ['s','min','h','j'], x:nb d'unités
    dataset = pd.read_csv(open(racine + 'Data_Blockchain.csv'), index_col=0) #import fichier csv brut
    
    dic={'s':1,'min':60,'h':3600,'j':3600*24}
    m = x*dic[u]
    can = 'candle_'+str(x)+u
    
    
    newCandle = pd.DataFrame(columns=['nbTrans','volume','fee']) 
    
    d_group = dataset.groupby(dataset['time']//m)

    
    #Remplissage des colonnes
    
    newCandle['nbTrans'] = d_group['nbTrans'].apply(lambda x: x.sum())
    newCandle['volume'] = d_group['volume'].apply(lambda x: x.sum())
    newCandle['fee'] = d_group['fee'].apply(lambda x: x.mean())
    
    newCandle = newCandle.drop(newCandle.index[-1]) #on supprime la dernière ligne qui est probablement incomplète
    
    newCandle.index = [int(x) for x in newCandle.index]
    newCandle.index.name = can
    
    #enregistrement du nouveau fichier
    newCandle.to_csv(racine +'Blockchain_'+can+'.csv')