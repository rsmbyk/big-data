import os
from operator import itemgetter

import flask
import pandas as pd

app = flask.Flask(__name__)

with open('data/user_map.txt') as infile:
    user_map = infile.read().split('\n')[:-1]
    user_map = map(str.split, user_map)
    user_map = {username:int(index) for index, username in user_map}
    reversed_user_map = {int(index):username for username, index in user_map.items()}


@app.route('/models')
def get_all_models():
    limit = flask.request.values.get('limit', 25)
    models = os.listdir('data/models')[:limit]
    models = map(os.path.splitext, models)
    models = map(itemgetter(0), models)
    return flask.jsonify(models=list(models))


@app.route('/users')
def get_all_users():
    limit = flask.request.values.get('limit', 25)
    return flask.jsonify(users=list(user_map.keys())[:limit])


@app.route('/get-friends', methods=['POST'])
def get_friends():
    model = flask.request.values['model']
    model_filename = '{}.csv'.format(os.path.join('data', 'models', model))

    if not os.path.exists(model_filename):
        return flask.jsonify(error='model does not exist')

    user = flask.request.values['username']

    if user not in user_map:
        return flask.jsonify(error='username does not exist')

    communities = pd.read_csv(model_filename)
    community = int(communities[communities['name'] == user_map[user]]['label'].iloc[0])
    friends = communities[communities['label'] == community]['name'].tolist()
    friends = list(map(reversed_user_map.__getitem__, friends))

    return flask.jsonify(user_id=user_map[user],
                         username=user,
                         friends=friends,
                         model_filename=model_filename)


@app.route('/get-members', methods=['POST'])
def get_members():
    model = flask.request.values['model']
    model_filename = '{}.csv'.format(os.path.join('data', 'models', model))

    if not os.path.exists(model_filename):
        return flask.jsonify(error='model does not exist')

    community = int(flask.request.values['community'])

    communities = pd.read_csv(model_filename)

    if community not in communities['label']:
        return flask.jsonify(error='community does not exist')

    members = communities[communities['label'] == community]['name'].tolist()
    members = list(map(reversed_user_map.__getitem__, members))

    return flask.jsonify(community=community,
                         members=members,
                         model_filename=model_filename)


@app.route('/get-community', methods=['POST'])
def get_community():
    model = flask.request.values['model']
    model_filename = '{}.csv'.format(os.path.join('data', 'models', model))

    if not os.path.exists(model_filename):
        return flask.jsonify(error='model does not exist')

    user = flask.request.values['username']

    if user not in user_map:
        return flask.jsonify(error='username does not exist')

    communities = pd.read_csv(model_filename)
    community = int(communities[communities['name'] == user_map[user]]['label'].iloc[0])

    return flask.jsonify(user_id=user_map[user],
                         username=user,
                         community=community,
                         model_filename=model_filename)
