import os
def scan_dir(d):
    for root, dirs, files in os.walk(d):
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                with open(path, 'r', encoding='utf-8') as f:
                    try:
                        lines = f.readlines()
                        for i, line in enumerate(lines):
                            if '.to_csv(' in line:
                                print(f"{path}:{i+1}: {line.strip()}")
                    except:
                        pass
scan_dir('/home/joao/Desktop/inteli.gente/intelli.gente_data_extraction/InteligenteEtl')
scan_dir('/home/joao/Desktop/inteli.gente/intelli.gente_data_extraction/test')
