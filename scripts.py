import os
import csv
import re
import requests
from random import shuffle
from bs4 import BeautifulSoup


class UnloadSort():

    def __init__(self, path, files, user, key):
        # Обязательные аргументы
        self.path = path
        self.files = files
        self.user = user
        self.key = key
        # Ввод регионов
        self.region_dict = {
                'msk': '213', 
                'spb': '2'
                }
        
        self.ready_limit = []
        
        self.region = ''
        self.domain = ''
        self.csv_list = {}
        self.len = 0
        self.sort_list = []
        self.counter_id = 0

        self.limit_domain = self.__limit()
        self.limit_int = 0
        self.counter_limit = 0

        self.cache_file = ''
        self.domain_cache = ''
        self.counter_limit_cache = 0


    def progress_bar(self, element):
        """
        Отображает прогресс файлы, которые обрабатываются
        """
        progress = f'{element}'
        print(progress, end='\r')


    def start_app(self):
        """
        Начинает работу класса, его вызываем в экземпляре.
        Здесь сконцентрированны все внутренние методы класса.
        """
        self.len = len(self.files)
        for file in self.files:
            self.cache_file = file

            self.write_csv_list()
            self.progress_bar(file)

        self.__sort_limit()

        counter = 0
        for key, list in self.csv_list.items():
            
            self.cache_file = key
            self.domain = list[-1]
            self.counter_limit = 0
            self.limit_int = self.limit_domain[self.domain]

            if self.domain_cache == self.domain:
                counter += 1
            else:
                self.domain_cache = self.domain
                counter = 0

            for row in list:
                if type(row) == type({}):
                    # Проверяет: Счётчик равен лимиту? Если да, то выбирает другой файл
                    if self.counter_limit == self.limit_int[counter]:
                        break

                    # Проверяет: кешированный счётчик лимитов превысел лимит по файлу?
                    self.counter_limit_cache += row['Очень точная частотность']
                    if self.counter_limit_cache > self.limit_int[counter]:
                        self.counter_limit_cache = self.counter_limit
                        continue

                    self.progress_bar(f'{key}: {row["Запрос"]}')
                    xml = self.request_xmlproxy(row)
                    self.parser_xml(xml, row)
            
            self.ready_limit.append(
                {"домен": key,
                 "лимиты набранные": self.counter_limit,
                 "лимиты всего": self.limit_int[counter]
                }
            )
                
        self.create_csv()
        self.create_csv_limit()


    def setup_region(self, file):
        """
        Метод вытягиевает регион из названия файла
        """
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
        """
        Чтение файлов csv из папки file_excel.
        Запись всех строк файла в словарь csv_list.

        Файлы должны обязательно называться так:
        my_domain.ru - сюда своё доменное имя
        orhanic.keys - сюда можно любые слова вставить, главное, чтобы были
        .msk - название регионов, которые записаны в словарь region_dict
        .csv - обязательно с делиметром (;)

        Пример:
        my_domain.ru.organic.keys.msk.csv
        """
        self.setup_region(self.cache_file)
        self.csv_list[self.cache_file] = []

        with open(
            f'{self.path + self.cache_file}', 
            'r', 
            encoding="utf8", 
            newline=''
            ) as csvfile:

            reader = csv.DictReader(csvfile, delimiter=';')
            for row in reader:
                if int(row['Позиция']) <= 30:
                    try:
                        request = row['\ufeffЗапрос']
                    except KeyError:
                        request = row['Запрос']

                    visit = round(int(row['Очень точная частотность'])/300) 

                    if not visit:
                        visit = 1

                    self.csv_list[self.cache_file].append(
                        {'Запрос': request,
                            'Страница': row['Страница'],
                            'Позиция': row['Позиция'],
                            'Очень точная частотность': visit,
                            'Регион': self.region,
                        }
                    )
            shuffle(self.csv_list[self.cache_file])
            self.csv_list[self.cache_file].append(
                self.search_my_domains()
            )


    def search_my_domains(self):
        """
        Вытягиевает домен из названия файла
        """
        pattern = r'(www\.)*([A-Za-z0-9-]{1,63})(\.[A-Za-z0-9-]{1,10})'

        match = re.search(pattern, self.cache_file)
        return match.group()


    def request_xmlproxy(self, row):
        """
        POST запрос на сервер xmlproxy.ru. Отправляем xml.
        Передаются имя юзера, токен ключ, которые введёте.
        """
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
        """
        Парсинг XML файла принятый из xmlproxy.ru. Ищем домен в первой 30.
        """
        soup = BeautifulSoup(xml_data, 'xml')
        try:
            domains = soup.find_all('domain')
        except:
            pass
        current = False
        for domain in domains:
            if str(self.domain).lower() in str(domain).lower():
                current = True
                break
        
        if current:
            self.counter_id += 1
            visit = row['Очень точная частотность']

            self.counter_limit += visit
            self.counter_limit_cache = self.counter_limit

            self.sort_list.append(
                {
                    'id': self.counter_id,
                    'домен': self.domain,
                    'запросы': row['Запрос'],
                    'доп приписка': '| ' + self.domain,
                    'кол-во заходов': visit,
                    'регион': row['Регион'],
                }
            )
    

    def create_csv(self):
        """
        Создаём итоговый файл с названием result.csv и записываем данные
        """
        with open('result.csv', 'w', encoding="cp1251", newline='') as csvfile:
            fieldnames = ['id', 
                          'домен', 
                          'запросы', 
                          'доп приписка', 
                          'кол-во заходов', 
                          'регион'
                          ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            for row in self.sort_list:
                writer.writerow(row)
    
    def create_csv_limit(self):
        """
        Создаём итоговый файл с тем, дошли ли все лимиты.
        """
        with open('limit_res.csv', 'w', encoding="cp1251", newline='') as csvfile:
            fieldnames = ['домен', 
                          'лимиты набранные', 
                          'лимиты всего'
                          ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            for row in self.ready_limit:
                writer.writerow(row)
    
    def __limit(self):
        """
        Чтение файла limit.csv и запись в словарь
        """
        dict_limit = {}

        with open(f'limit.csv', 'r', encoding="cp1251", newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            
            for row in reader:
                dict_limit[row['Домен']] = int(row['Лимиты'])

        return dict_limit
    
    def __sort_limit(self):
        """
        Формируем списко лимитов для каждого файла из папки file_excel,
        основываясь на лимитах на домен из limit.csv.
        """
        counter = 0
        list_limit = []
        self.csv_list["empty"] = [False]
        for list in self.csv_list.values():
            self.domain = list[-1]

            if self.domain_cache == '':
                self.domain_cache = self.domain
                counter += 1
            elif self.domain_cache == self.domain:
                counter += 1
            else:
                target_int = self.limit_domain[self.domain_cache] / counter
                round_int = round(target_int)
                diff_int = target_int - round_int
                range_int = round(diff_int * counter)


                for int in range(counter):
                    list_limit.append(round_int)

                if range_int < 0:
                    range_int = range_int*(-1)
                    enum_element = -1
                else:
                    enum_element = 1

                for id in range(range_int):
                    list_limit[id] += enum_element

                shuffle(list_limit)
                self.limit_domain[self.domain_cache] = list_limit
                list_limit = []

                self.domain_cache = self.domain
                counter = 1
                continue
        self.csv_list.pop('empty')
        self.domain_cache = ''


def main():
    path = os.getcwd() + "/file_excel/"
    user = "dnl.melnikov"
    key = "MTY3OTE0NDIyODcyODY5NDcxNDIxOTczMTM5"
    
    for files in os.walk(path):
        files = files[2]

    files.sort()

    demo = UnloadSort(path, files, user, key)
    demo.start_app()


main()
