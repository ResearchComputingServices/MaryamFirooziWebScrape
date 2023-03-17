import pandas as pd

#df = pd.read_pickle('./OutputData/Aecon_Group.pkl')                                 
df = pd.read_pickle('./OutputData/Advantage_Energy_Ltd..pkl')   

for index, row in df.iterrows():
    keywordsList = row['Keywords']
    url = row['URL']
    text = row['Text']
    if len(keywordsList) > 0:
        print(index, url, len(text), keywordsList)
        input('Presss ENTER to continue...')
