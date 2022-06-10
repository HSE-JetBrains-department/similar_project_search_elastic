# elastic_search
В этом репозитории находится часть с клиентом elasticsearch и поиском проектов.

## Установка

### Elasticsearch
Инструкция по установке: [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html)

### Пакеты python
Для установки пакетов нужно из корневой папки проекта запустить в терминале команду:
```shell
pip install .
```
### Необязательные элементы
Дополнительно можно установить [kibana](https://www.elastic.co/downloads/kibana), этот инструмент позволяет отправлять запросы напрямую к Elasticsearch, для работы проекта *не* требуется

## Использование
Для работы проекта *обязательно* должен быть запущен клиент Elasticsearch

Посмотреть основные доступные команды:
```shell
python run.py --help
```
Создать индекс:
```shell
python run.py create INDEX_NAME
```
Удалить индекс:
```shell
python run.py delete INDEX_NAME
```
Добавить предобработанные репозитории в индекс:
```shell
python run.py add INDEX_NAME PATH_TO_JSONS
```
Основная функция поиска в индексе по ссылке на github искомого проекта:
```shell
python run.py add INDEX_NAME LINK_OF_REPO
```
