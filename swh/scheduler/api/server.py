# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from flask import request, Flask

from swh.core import config
from swh.core.api import (decode_request,
                          error_handler,
                          encode_data_server as encode_data)

from swh.core.api import negotiate, JSONFormatter, MsgpackFormatter
from swh.scheduler import get_scheduler as get_scheduler_from
from swh.scheduler import DEFAULT_CONFIG, DEFAULT_CONFIG_PATH


app = Flask(__name__)
scheduler = None


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


def get_sched():
    global scheduler
    if not scheduler:
        scheduler = get_scheduler_from(**app.config['scheduler'])
    return scheduler


def has_no_empty_params(rule):
    return len(rule.defaults or ()) >= len(rule.arguments or ())


@app.route('/')
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def index():
    return 'SWH Scheduler API server'


@app.route('/close_connection', methods=['GET', 'POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def close_connection():
    return get_sched().close_connection()


@app.route('/set_status_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def set_status_tasks():
    return get_sched().set_status_tasks(**decode_request(request))


@app.route('/create_task_type', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def create_task_type():
    return get_sched().create_task_type(**decode_request(request))


@app.route('/get_task_type', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def get_task_type():
    return get_sched().get_task_type(**decode_request(request))


@app.route('/get_task_types', methods=['GET', 'POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def get_task_types():
    return get_sched().get_task_types(**decode_request(request))


@app.route('/create_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def create_tasks():
    return get_sched().create_tasks(**decode_request(request))


@app.route('/disable_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def disable_tasks():
    return get_sched().disable_tasks(**decode_request(request))


@app.route('/get_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def get_tasks():
    return get_sched().get_tasks(**decode_request(request))


@app.route('/get_task_runs', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def get_task_runs():
    return get_sched().get_task_runs(**decode_request(request))


@app.route('/search_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def search_tasks():
    return get_sched().search_tasks(**decode_request(request))


@app.route('/peek_ready_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def peek_ready_tasks():
    return get_sched().peek_ready_tasks(**decode_request(request))


@app.route('/grab_ready_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def grab_ready_tasks():
    return get_sched().grab_ready_tasks(**decode_request(request))


@app.route('/schedule_task_run', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def schedule_task_run():
    return get_sched().schedule_task_run(**decode_request(request))


@app.route('/mass_schedule_task_runs', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def mass_schedule_task_runs():
    return get_sched().mass_schedule_task_runs(**decode_request(request))


@app.route('/start_task_run', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def start_task_run():
    return get_sched().start_task_run(**decode_request(request))


@app.route('/end_task_run', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def end_task_run():
    return get_sched().end_task_run(**decode_request(request))


@app.route('/filter_task_to_archive', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def filter_task_to_archive():
    return get_sched().filter_task_to_archive(**decode_request(request))


@app.route('/delete_archived_tasks', methods=['POST'])
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def delete_archived_tasks():
    return get_sched().delete_archived_tasks(**decode_request(request))


@app.route("/site-map")
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def site_map():
    links = []
    sched = get_sched()
    for rule in app.url_map.iter_rules():
        if has_no_empty_params(rule) and hasattr(sched, rule.endpoint):
            links.append(dict(
                rule=rule.rule,
                description=getattr(sched, rule.endpoint).__doc__))
    # links is now a list of url, endpoint tuples
    return links


api_cfg = None


def run_from_webserver(environ, start_response,
                       config_path=DEFAULT_CONFIG_PATH):
    """Run the WSGI app from the webserver, loading the configuration."""
    global api_cfg
    if not api_cfg:
        api_cfg = config.load_named_config(config_path, DEFAULT_CONFIG)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app(environ, start_response)


if __name__ == '__main__':
    print('Please use the "swh-scheduler api-server" command')
