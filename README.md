# saver - сохранение данных из базы данных в различные форматы

## Установка

1. Установить [python3](https://python3.org) (проверено на 3.8+)
2. Скачать содержимое репозитория и открыть терминал в папке
3. Установить зависимости через `python -m pip install -r requirements.txt`
4. Запустить через `python saver.py запрос/название_таблицы`

## Параметры запуска

Единственный принимаемый аргумент - запрос, либо название таблицы.
Если он не указан (а также не указаны команды получения списка таблиц или получения свойств заданной таблицы), то программа завершается ошибкой

### Параметры подключения к базе данных

Могут быть установлены через следующие аргументы:
- --db_addr / -H - адрес сервера (по-умолчанию *localhost*)
- --db_port / -P - порт сервера (по-умолчанию *5342*)
- --db_name / -D - название базы данных (по-умолчанию *city_db_final*)
- --db_user / -U - имя пользователя (по-умолчанию *postgres*)
- --db_pass / -W - пароль пользователя (по-умолчанию *postgres*)

Кроме того, эти же параметры могут быть заданы через переменные окружения, пример - [env_default](env_default.txt) .  
Подгрузить переменные окружения из файла под Linux можно командой `source env_default.txt` .
Для удобства рекомендуется скопировать и изменить файл, назвав его **env**

### Варианты работы, отличные от выгрузки данных

Если задан хотя бы один из этих аргументов, то запрос обработан не будет, даже если он задан.  
Список таблиц стоит выше по приоритету, чем получение информации о таблице.

- --list_tables / -l - получение списка таблиц в базе данных
- --describe_table / -d - получение информации о выбранной таблице

### Прочие параметры

- --geometry_column / -g - установка столбца геометрии (для вывода в geojson)
- --use_centroids / -c (флаг) - сохранять только центроиды объектов (для выгрузки таблицы, с запросом ничего не делает)
- --verbose_level / -v - уровень работы логгера (ERROR, WARNING, INFO, DEBUG)
- --filename / -f - сохранение в файл, формат по расширению (.csv, .xlsx, .geojson или .json). Для сохранения в geojson необходимо указать столбец геометрии (по-умолчанию *goemetry*)