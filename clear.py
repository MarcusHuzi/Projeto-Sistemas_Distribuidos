import json
import os, shutil

#liberar arquivos com metadados
with open("file_block.json", "w") as fb:
    json.dump({}, fb)
with open("block_minion.json", "w") as fb:
    json.dump({}, fb)

#liberar diretorios com os arquivos
for folder in ['minion0', 'minion1', 'minion2']:
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


  