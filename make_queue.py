from os import rename
import synapseclient
syn = synapseclient.Synapse()
syn.login()

Manifests = syn.tableQuery("SELECT * FROM syn20446927 WHERE (\"name\"='synapse_storage_manifest.csv')").asDataFrame()

ImagingLevel2 = syn.tableQuery("SELECT * FROM syn20446927 WHERE ((\"Component\"='ImagingLevel2') AND (\"benefactorId\"!='HTAN HTAPP'))").asDataFrame()


ome_tiff_regex = '.*\.ome.tif{1,2}$'
ome_tiffs = ImagingLevel2[ImagingLevel2.name.str.match(ome_tiff_regex)]
ome_tiffs

ome_tiffs = ome_tiffs.assign(minerva = '', thumbnail = '')
ome_tiff_synids = ome_tiffs.loc[:,('id', 'minerva','thumbnail')]
ome_tiff_synids.rename(columns = {'id':'synapseId'}, inplace = True)

ome_tiff_synids.to_csv('ome_tiff_synids.csv',index = False, header = True)