import os
import csv
import re
import requests
from bs4 import BeautifulSoup

path = os.getcwd() + "\\file_excel\\"

for files in os.walk(path):
    files = files[2]


def setup_region(file):
    region_dict = {
        'msk': '213', 
        'spb': '2'
        }
    
    regex = r"\.(.+?){3}\."
    matches = re.finditer(regex, file, re.MULTILINE)

    for match in matches:
        try:
            region = region_dict[match.group()[1:-1]]
            return region
        except KeyError:
            pass
    

def write_csv_list(file):
    region = setup_region(file)
    csv_list = []

    counter = 0

    with open(f'{path + file}', 'r', encoding="utf8", newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')

        for row in reader:
            if int(row['Позиция']) < 30:
                try:
                    request = row['\ufeffЗапрос']
                except KeyError:
                    request = row['Запрос']
                csv_list.append(
                    {'Запрос': request,
                        'Страница': row['Страница'],
                        'Позиция': row['Позиция'],
                        'Очень точная частотность': row['Очень точная частотность'],
                        'Регион': '&lr=' + region,
                    }
                )

                counter += 1

            if counter > 5:
                return csv_list, region

pre_data_csv = write_csv_list(files[1])
print(pre_data_csv[0])

url = 'http://xmlproxy.ru/search/xml?user=dnl.melnikov%40yandex.ru&key=50e40a0516bd3825b4a6cecb43594cb1' + pre_data_csv[1]
for page in range(3):
    print(page)
# for page in range(3):
#     data = f'''
#     <?xml version="1.0" encoding="UTF-8"?> 
#     <request>   
#         <query>сеотемпл</query>
#         <sortby>rlv</sortby>
#         <maxpassages>2</maxpassages>
#         <page>{page}</page> 
#         <groupings>
#             <groupby attr="d" mode="deep" groups-on-page="10" docs-in-group="3" /> 
#         </groupings>        
#     </request>
#     '''

#     req = requests.post(url=url, data=data)


