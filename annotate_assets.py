import synapseclient
import pandas as pd
syn = synapseclient.Synapse()
syn.login()


succeeded = pd.read_csv('tmp/succeeded.csv')

for i in range(len(succeeded)):
    synid = succeeded.iloc[i,0]
    key = succeeded.iloc[i,1]
    value = succeeded.iloc[i,3]
    annos = syn.get_annotations(synid)
    annos[key] = value
    syn.set_annotations(annos)