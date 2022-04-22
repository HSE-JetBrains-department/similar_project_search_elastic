import ElasticClass
import json

es = ElasticClass.ElasticLoader()
body = {
    "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 1
    },
    'mappings': {
        'examplecase': {
            'properties': {
                'tbl_id': {'type': 'keyword'},
                'texts': {'type': 'text'},
            }
        }
    }
}
# es.es.create("new_fprmat_test", body)
es.create_index("new_format_100", None, 1, 'jsons2/jsons10000')


# doc = json.load(open('jsons3/Chassis_Chassis.json'))
# print(doc)
# es.add_by_json(doc, 'new_fprmat_test')
