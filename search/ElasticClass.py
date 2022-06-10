# from distutils.log import error
# from re import M
# from multipledispatch import dispatch
import json
import os
import spacy
import warnings
import math
import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch import exceptions as errors

warnings.filterwarnings("ignore")  # Разумеется, это временно
LOG = 0


class ElasticLoader:
    """
        ElasticLoader provides functionality for creating an index and searching
        through it using the ElasticSearch API
        By default, the index is called similar_projects, it is stored in the index_ value
    """

    def __init__(self, host="http://localhost", port=9200):
        """
            Connecting to the elastic search server using your own host and port
            By default, it is connected locally to http://localhost:9200
        """

        self.es = Elasticsearch(HOST=host, PORT=port, timeout=100)
        if not self.es.ping():
            print("Connection to " + host + ":" + str(port) + " failed")
            exit()

    def create_index(self, index: str):
        """
            Simply creates an empty index with settings

        :param index: the name of index
        """

        try:
            with open('search/index_settings/index_mappings.json', 'r') as mappings_file:
                mappings_dict = json.loads(mappings_file.read())
            with open('search/index_settings/index_settings.json', 'r') as settings_file:
                settings_dict = json.loads(settings_file.read())
            mappings = {
                "mappings": {
                    "properties": {
                        "owner": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "keyword"
                        },
                        "readme": {
                            "type": "text",
                            "analyzer": "readme_analyzer",
                            "search_analyzer": "readme_analyzer"
                        },
                        "languages": {
                            "type": "keyword"
                        },
                        "percentages": {
                            "type": "keyword"
                        },
                        "imports": {
                            "properties": {
                                "count": {
                                    "type": "integer"
                                },
                                "key": {
                                    "type": "text",
                                    "analyzer": "readme_analyzer",
                                    "search_analyzer": "readme_analyzer"
                                }
                            }
                        },
                        "identifiers": {
                            "properties": {
                                "count": {
                                    "type": "integer"
                                },
                                "key": {
                                    "type": "text",
                                    "analyzer": "readme_analyzer",
                                    "search_analyzer": "readme_analyzer"
                                }
                            }
                        },
                        "splitted_identifiers": {
                            "properties": {
                                "count": {
                                    "type": "integer"
                                },
                                "key": {
                                    "type": "text",
                                    "analyzer": "readme_analyzer",
                                    "search_analyzer": "readme_analyzer"
                                }
                            }
                        },
                        "docstrings": {
                            "type": "text"
                        },
                        "stargazers_count": {
                            "type": "keyword"
                        },
                        "commit_sha": {
                            "type": "keyword"
                        },
                        "repo_id": {
                            "type": "keyword"
                        }
                    }
                }
            }
            settings = {
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "readme_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "english_stop",
                                    "russian_stop",
                                    "chineese_stop",
                                    "snowball_english"
                                ],
                                "char_filter": [
                                    "number_filter"
                                ]
                            },
                            "imports_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "snowball_english"
                                ]
                            }
                        },
                        "filter": {
                            "english_stop": {
                                "type": "stop",
                                "stopwords": "_english_"
                            },
                            "russian_stop": {
                                "type": "stop",
                                "stopwords": "_russian_"
                            },
                            "chineese_stop": {
                                "type": "stop",
                                "stopwords": "_cjk_"
                            },
                            "my_snow": {
                                "type": "snowball",
                                "language": "English"
                            },
                            "snowball_english": {
                                "type": "condition",
                                "filter": [
                                    "my_snow"
                                ],
                                "script": {
                                    "source": "token.getTerm().length() > 5"
                                }
                            }
                        },
                        "char_filter": {
                            "number_filter": {
                                "type": "pattern_replace",
                                "pattern": "\\d+",
                                "replacement": ""
                            }
                        }
                    }
                }
            }
            if not self.index_exists(index):
                self.es.indices.create(index=index, body={**mappings_dict, **settings_dict})
                return f"Created index called \"{index}\""
            else:
                return f"Index \"{index}\" already exists"
        except errors.RequestError as e:
            return e
            # print("Index " + index + " already exists")

    def delete_index(self, index: str) -> str:
        """
            Delete the index

        :param index: the name of index
        """

        try:
            if self.index_exists(index=index):
                if input(f"Delete \"{index}\" index? Y/n: ") in "Yy":
                    self.es.indices.delete(index=index)
                    return f"Index \"{index}\" deleted"
            else:
                return f"Index \"{index}\" does not exist"
        except errors.NotFoundError:
            return f"Index \"{index}\" does not exist"

    def update_mappings(self, index: str):
        imports_mappings = {
            "properties": {
                "count": {
                    "type": "integer"
                },
                "key": {
                    "type": "text",
                    # "analyzer": "readme_analyzer",
                    # "search_analyzer": "readme_analyzer"
                }
            }
        }
        identifiers_mappings = {
            "properties": {
                "count": {
                    "type": "integer"
                },
                "key": {
                    "type": "text",
                    # "analyzer": "readme_analyzer",
                    # "search_analyzer": "readme_analyzer"
                }
            }
        }

        try:
            self.es.indices.put_mapping(index=index, doc_type="imports", body=imports_mappings)
        except elasticsearch.exceptions.RequestError as e:
            print(e)

        try:
            self.es.indices.put_mapping(
                index="index", doc_type="identifiers", body=identifiers_mappings)
        except elasticsearch.exceptions.RequestError as e:
            print(e)

    def index_exists(self, index: str) -> bool:
        """
            Check if index exists

        :param index: the name of index
        """

        return self.es.indices.exists(index=index)

    def repo_exists(self, index: str, link: str):
        """
            Check if repo exists in index

        :param index: the name of index
        :param link: link of repository to find
        """

        substr = link[len('https://github.com/'):]
        owner = substr[:substr.find('/')]
        name = substr[substr.find('/') + 1:]
        repo = self.es.search(index=index, body={
            'query': {
                "dis_max": {
                    "queries": [
                        {"match": {"owner": owner}},
                        {"match": {"name": name}}
                    ]
                }
            }
        })
        if len(repo['hits']['hits']) == 0:
            return None
        elif repo['hits']['hits'][0]['_source']['owner'] == owner \
                and repo['hits']['hits'][0]['_source']['name'] == name:
            return repo['hits']['hits'][0]['_source']

    def add_jsons(self, index: str, directory: str, count: int) -> str:
        """
            Add group of jsons to index

        :param index: the name of index
        :param directory: path to jsons
        :param count: number of jsons to add
        """

        if not self.index_exists(index=index):
            return f"Index \"{index}\" does not exist"
        files_added = 0
        for file in os.listdir(directory):
            if file.endswith('.json'):
                path = directory + '/' + file
                # print(file + ",    SIZE:", os.path.getsize(path))
                if os.path.getsize(path) > 100000:
                    continue
                doc = json.load(open(path))
                nlp = spacy.load("en_core_web_sm")
                for i in range(len(doc['readme'])):
                    text = doc['readme'][i]
                    readme = nlp(text)
                    clear_tokens = self.clear_nums(readme)
                    doc['readme'][i] = ' '.join(clear_tokens)
                d_imports = []
                for key in doc['imports']:
                    d_imports.append({"key": key, "count": doc["imports"][key]})
                d_identifiers = []
                for key in doc['identifiers']:
                    d_identifiers.append({"key": key, "count": doc["identifiers"][key]})
                d_splitted_identifiers = []
                for key in doc['splitted_identifiers']:
                    d_splitted_identifiers.append(
                        {"key": key, "count": doc["splitted_identifiers"][key]})
                doc["imports"] = d_imports
                doc["identifiers"] = d_identifiers
                doc["splitted_identifiers"] = d_splitted_identifiers
                print(self.add_by_json(json_file=doc, index=index))
                files_added += 1
                if files_added == count:
                    break
        return f"Added {files_added} repositories to \"{index}\" index"

    def add_by_json(self, index: str, json_file: dict) -> str:
        """
            Adds and element to the index using json (python dict) file

        :param index: the name of index
        :param json_file: python dict with description of the new element
        """

        try:
            self.es.index(index=index, document=json_file)
            link = f"Added https://github.com/{json_file['owner']}/{json_file['name']}"
            return link
        except errors.TransportError as e:
            return e.info

    def add_by_url(self, index: str, url: str):
        self.add_by_json(index=index, json_file=self.get_json(url=url))

    def add_by_url_list(self, index: str, u_list: list):
        for url in u_list:
            try:
                my_json = self.get_json(url)
                self.add_by_json(index=index, json_file=my_json)
            except RuntimeError:
                return 1
        return 0

    @staticmethod
    def get_json(url):
        return {}

    @staticmethod
    def clear_nums(tokens) -> list:
        """
            Delete ".", ",", ":", "/" from tokens

        :param tokens: list of tokens
        """

        clear_tokens = []
        for token in tokens.ents:
            token_copy = str(token)
            token = str(token).replace('.', '')
            token = str(token).replace(',', '')
            token = str(token).replace(':', '')
            token = str(token).replace('/', '')
            if not token.isnumeric():
                clear_tokens.append(token_copy)
        return clear_tokens

    def pop(self, id_):
        pass

    def get(self):
        pass

    def get_by_repo(self,
                    index: str,
                    repo: dict,
                    boosts: dict = None,
                    hits_size: int = 10) -> list:
        """
            Searching by repository (dictionary)

        :param index: index name
        :param repo: json of repository
        :param boosts: dictionary with info about boost parameter for each field of search
        :param hits_size: how many results input in search
        :return: python list of found elements (dictionaries)
        """

        strictness = "should"
        body = {
            "query": {
                "bool": {
                    strictness: []
                }
            }
        }
        if 'imports' in repo.keys():
            for imp in repo['imports']:
                imp_boost = 1 if 'imports' not in boosts else boosts['imports']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "imports.key": {
                            "query": imp["key"],
                            "boost": math.sqrt(imp["count"]) * imp_boost,
                        },
                    }
                })
        if 'identifiers' in repo.keys():
            for idf in repo['identifiers']:
                idf_boost = 1 if 'identifiers' not in boosts else boosts['identifiers']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "identifiers.key": {
                            "query": idf["key"],
                            "boost": math.sqrt(idf["count"]) * idf_boost
                        }
                    }
                })
        if 'splitted_identifiers' in repo.keys():
            for spl_idf in repo['splitted_identifiers']:
                if 'splitted_identifiers' not in boosts:
                    spl_idf_boost = 1
                else:
                    spl_idf_boost = boosts['splitted_identifiers']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "splitted_identifiers.key": {
                            "query": spl_idf["key"],
                            "boost": math.sqrt(spl_idf["count"]) * spl_idf_boost
                        }
                    }
                })
        if 'languages' in repo.keys():
            for lang in repo['languages']:
                lang_boost = 1 if 'languages' not in boosts else boosts['languages']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "languages": {
                            "query": lang,
                            "boost": lang_boost,
                        }
                    }
                })
        if "readme" in repo.keys():
            for rdm in repo['readme']:
                rdm_boost = 1 if 'readme' not in boosts else boosts['readme']
                body["query"]['bool'][strictness].append({
                    "match": {
                        "readme": {
                            "query": rdm,
                            "boost": rdm_boost,
                        },
                    }
                })
        # print(body, '\n\n\n\n')
        res = self.es.search(index=index, body=body, size=hits_size)
        print(f"Found {len(res['hits']['hits'])} repositories:")
        array = []
        if LOG:
            with open('body.txt', 'w+') as file:
                file.write(str(body))

        for i in range(max(0, len(res['hits']['hits']))):
            if 'name' in res['hits']['hits'][i]['_source']:
                link = 'https://github.com/' + \
                       res['hits']['hits'][i]['_source']['owner'] + \
                       '/' + res['hits']['hits'][i]['_source']['name']
                array.append(link)
                if LOG:
                    with open('info' + str(i) + '.txt', 'w+') as file:
                        file.write(str(res['hits']['hits'][i]))
        # print("FOUND", len(array), "ANSWERS IN INDEX", index)
        return array

    def find_by_link(self, index: str, link: str, hits_size: int) -> str:
        """
            Find similar repositories by link

        :param index: the name of index
        :param link: link of repository to find
        :param hits_size: how many results input in search
        """

        if not self.index_exists(index=index):
            return f"Index \"{index}\" does not exist"

        boosts = {'imports': 1,
                  'identifiers': 1,
                  'splitted_identifiers': 6,
                  'languages': 1,
                  'readme': 1}
        repo = self.repo_exists(index=index, link=link)
        if not repo:
            return f"{link} is not in index"
        else:
            result = self.get_by_repo(index=index, repo=repo, boosts=boosts, hits_size=hits_size)
            for link in result:
                print(link)
            return "Success"
