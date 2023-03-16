import os
import csv
import re
import requests
from bs4 import BeautifulSoup


class UnloadSort():

    def __init__(self, path, files, user, key):
        self.path = path
        self.files = files
        self.user = user
        self.key = key

        self.region_dict = {
                'msk': '213', 
                'spb': '2'
                }
        
        self.region = ''
        self.csv_list = {}
        self.cache_file = ''
        self.sort_list = []
        self.counter_id = 0


    def start_app(self):
        for file in self.files:
            self.cache_file = file

            self.write_csv_list()

        for key, list in self.csv_list.items():
            self.cache_file = key

            for row in list:
                if type(row) == type({}):
                    xml = self.request_xmlproxy(row)
                    self.parser_xml(xml, row)

        self.create_csv()


    def setup_region(self, file):
        
        regex = r"\.(.+?){3}\."
        matches = re.finditer(regex, file, re.MULTILINE)

        for match in matches:
            try:
                region = self.region_dict[match.group()[1:-1]]
                self.region = '&lr=' + region
                return True
            except KeyError:
                pass
    

    def write_csv_list(self):
        self.setup_region(self.cache_file)
        self.csv_list[self.cache_file] = []
        #счётчик для ограничения запросов
        counter = 1

        with open(
            f'{self.path + self.cache_file}', 
            'r', 
            encoding="utf8", 
            newline=''
            ) as csvfile:

            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                if int(row['Позиция']) < 30:
                    try:
                        request = row['\ufeffЗапрос']
                    except KeyError:
                        request = row['Запрос']
                    self.csv_list[self.cache_file].append(
                        {'Запрос': request,
                            'Страница': row['Страница'],
                            'Позиция': row['Позиция'],
                            'Очень точная частотность': row['Очень точная частотность'],
                            'Регион': self.region,
                        }
                    )

                    counter += 1
                # ограничитель для тестов
                if counter > 5:
                    self.csv_list[self.cache_file].append(
                        self.search_my_domains()
                        )
                    return True


    def search_my_domains(self):
        pattern = r'(www\.)*([A-Za-z0-9-]{1,63})(\.[A-Za-z0-9-]{1,10})'

        match = re.search(pattern, self.cache_file)
        return match.group()


    def request_xmlproxy(self, row):
        url = f'http://xmlproxy.ru/search/xml?user={self.user}%40yandex.ru&key={self.key}' + self.region

        headers = {'Content-Type': 'application/xml; charset=utf-8'}

        data = f'''<?xml version="1.0" encoding="UTF-8"?> 
            <request>   
                <query>{row['Запрос']}</query>
                <sortby>rlv</sortby>
                <maxpassages>0</maxpassages>
                <page>0</page> 
                <groupings>
                    <groupby attr="d" mode="deep" groups-on-page="30" docs-in-group="1" /> 
                </groupings>        
            </request>
            '''

        return requests.post(
            url=url, 
            data=data.encode('utf-8'), 
            headers=headers
            ).content


    def parser_xml(self, xml_data, row):
        soup = BeautifulSoup(xml_data, 'xml')
        need_domain = self.csv_list[self.cache_file][-1]
        try:
            domains = soup.find_all('domain')
        except:
            pass
        current = False
        for domain in domains:
            if str(need_domain).lower() in str(domain).lower():
                current = True
                break
        
        if current:
            self.counter_id += 1
            visit = round(int(row['Очень точная частотность'])/300) 

            if not visit:
                visit = 1

            self.sort_list.append(
                {
                    'id': self.counter_id,
                    'домен': need_domain,
                    'запросы': row['Запрос'],
                    'доп приписка': '| ' + need_domain,
                    'кол-во заходов': visit,
                    'регион': row['Регион'],
                }
            )
    

    def create_csv(self):
        with open('result.csv', 'w', newline='') as csvfile:
            fieldnames = ['id', 'домен', 'запросы', 'доп приписка', 'кол-во заходов', 'регион']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            for row in self.sort_list:
                writer.writerow(row)


def main():
    path = os.getcwd() + "\\file_excel\\"
    user = "dnl.melnikov"
    key = "MTY3ODkzMDY3NTQ5NjIyMjIwNTgxOTczMTE2"

    for files in os.walk(path):
        files = files[2]

    demo = UnloadSort(path, files, user, key)
    demo.start_app()

main()