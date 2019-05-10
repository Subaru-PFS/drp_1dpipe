import requests
import json


class DummyNotifier:
    """Pipeline watcher dummy interface"""
    def __init__(self, api_url=None, name=None, nodes=None):
        self.pipeline_url = None

    def update(self, node, state=None, children=None):
        pass


class Notifier:
    def __init__(self, api_url, name=None, nodes=None):
        self.api_url = api_url
        self.name = name
        self.nodes = nodes
        pipeline = {'name': name,
                    'nodes': nodes}
        if api_url:
            r = requests.post('{}/pipelines'.format(api_url),
                              headers={'content-type': 'application/json'},
                              data=json.dumps(pipeline))
            if r.status_code != 201:
                raise Exception("Can't create pipeline watcher")
            self.pipeline_url = '{}/pipelines/{}'.format(api_url,
                                                         json.loads(r.text))

    def update(self, node, state=None, children=None):
        """Update a node"""
        if not self.api_url or not (state or children):
            # nothing to do
            return
        req = {'_id': node}
        if state:
            req['state'] = state
        if children:
            req['children'] = children
        requests.put(self.pipeline_url,
                     headers={'content-type': 'application/json'},
                     data=json.dumps([req]))
