import os
import sys

from .cluster import *

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY2:
    STRING = (str, unicode)
if PY3:
    STRING = (str, bytes)


def __items2list(items):
    t = []
    for item in items:
        m = {}
        for k in item.keys():
            m[k] = item.get(k)
        t.append(m)
    return t


def list_jobs(client):
    # client = _get_client()
    arr = __list_jobs(client)
    return {'Items': arr, 'Marker': ''}


def __list_jobs(client, marker='', maxItemCount=100):
    t = []
    result = client.list_jobs(marker, maxItemCount)
    if result.Items:
        t = t + __items2list(result.Items)
    if result.NextMarker and result.NextMarker != '':
        arr = __list_jobs(client, result.NextMarker, maxItemCount)
        t = t + arr
    return t


def filter_list(arr, filters):
    '''
    fitlers:
       1. {State:['Running','Waiting']}
       2. {NumRunningInstance: {'>': 1}}
       3. {JobName: {'like':'jobName1'}}
       4. {JobName: {'startswith':'qsub-'}}
       5. {JobName: {'startswith':['qsub-user1-','qsub-user2-']}}
    '''
    t = arr

    for k in filters:

        if isinstance(filters[k], STRING):
            t = [item for item in t if filters[k].lower() == item.get(k).lower()]
        if isinstance(filters[k], (list, tuple)):
            alist = [x.lower() for x in filters[k]]
            t = [item for item in t if (
                item.get(k) != None and item.get(k).lower() in alist)]
        if isinstance(filters[k], dict):

            startswith = filters[k].get('startswith')

            like = filters[k].get('like')

            gt = filters[k].get('>')
            lt = filters[k].get('<')

            if startswith != None:

                startswith = [startswith] if isinstance(
                    startswith, STRING) else startswith
                startswith = map(lambda x: x.lower(), startswith)
                t2 = []
                for item in arr:
                    for ss in startswith:
                        if item.get(k).lower().startswith(ss):
                            t2.append(item)
                t = t2

            elif like != None:
                like = like.lower()
                t = [item for item in t if like in item.get(k).lower()]
            elif (gt != None or lt != None):
                t = [item for item in t
                     if (gt == None or gt != None and item.get(k) > gt) or (lt == None or lt != None and item.get(k) < lt)]
            else:
                t = arr
        arr = t
    return t


def to_dict(item):
    m = {}
    for k in item.keys():
        m[k] = item.get(k)
    return m


def items2arr(items):
    t = []
    for item in items:
        m = to_dict(item)
        t.append(m)
    return t
