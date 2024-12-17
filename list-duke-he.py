import synapseclient
import synapseutils
import re

syn = synapseclient.Synapse(silent = True)
syn.login()

walkedPath = synapseutils.walk(syn, 'syn25892951')

for dirpath, dirname, filename in walkedPath:
    for f in filename:
        if re.match(r'.+\.tiff', f[0]):
            print(f[1])