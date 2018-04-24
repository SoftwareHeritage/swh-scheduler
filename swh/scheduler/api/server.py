# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import click

from flask import g, request

from swh.core import config
from swh.scheduler import get_scheduler
from swh.core.api import (SWHServerAPIApp, decode_request,
                          error_handler,
                          encode_data_server as encode_data)

DEFAULT_CONFIG_PATH = 'backend/scheduler'
DEFAULT_CONFIG = {
    'scheduler': ('dict', {
        'cls': 'local',
        'args': {
            'scheduling_db': 'dbname=softwareheritage-scheduler-dev',
        },
    })
}


app = SWHServerAPIApp(__name__)


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.before_request
def before_request():
    g.scheduler = get_scheduler(**app.config['scheduler'])


@app.route('/')
def index():
    return 'SWH Scheduler API server'


@app.route('/create_task_type', methods=['POST'])
def create_task_type():
    return encode_data(g.scheduler.create_task_type(**decode_request(request)))


@app.route('/get_task_type', methods=['POST'])
def get_task_type():
    return encode_data(g.scheduler.get_task_type(**decode_request(request)))


@app.route('/get_task_types', methods=['POST'])
def get_task_types():
    return encode_data(g.scheduler.get_task_types(**decode_request(request)))


@app.route('/create_tasks', methods=['POST'])
def create_tasks():
    return encode_data(g.scheduler.create_tasks(**decode_request(request)))


@app.route('/disable_tasks', methods=['POST'])
def disable_tasks():
    return encode_data(g.scheduler.disable_tasks(**decode_request(request)))


@app.route('/get_tasks', methods=['POST'])
def get_tasks():
    return encode_data(g.scheduler.get_tasks(**decode_request(request)))


@app.route('/peek_ready_tasks', methods=['POST'])
def peek_ready_tasks():
    return encode_data(g.scheduler.peek_ready_tasks(**decode_request(request)))


@app.route('/grab_ready_tasks', methods=['POST'])
def grab_ready_tasks():
    return encode_data(g.scheduler.grab_ready_tasks(**decode_request(request)))


@app.route('/schedule_task_run', methods=['POST'])
def schedule_task_run():
    return encode_data(g.scheduler.schedule_task_run(
        **decode_request(request)))


@app.route('/mass_schedule_task_runs', methods=['POST'])
def mass_schedule_task_runs():
    return encode_data(
        g.scheduler.mass_schedule_task_runs(**decode_request(request)))


@app.route('/start_task_run', methods=['POST'])
def start_task_run():
    return encode_data(g.scheduler.start_task_run(**decode_request(request)))


@app.route('/end_task_run', methods=['POST'])
def end_task_run():
    return encode_data(g.scheduler.end_task_run(**decode_request(request)))


@app.route('/filter_task_to_archive', methods=['POST'])
def filter_task_to_archive():
    return encode_data(
        g.scheduler.filter_task_to_archive(**decode_request(request)))


@app.route('/delete_archived_tasks', methods=['POST'])
def delete_archived_tasks():
    return encode_data(
        g.scheduler.delete_archived_tasks(**decode_request(request)))


def run_from_webserver(environ, start_response,
                       config_path=DEFAULT_CONFIG_PATH):
    """Run the WSGI app from the webserver, loading the configuration."""
    cfg = config.load_named_config(config_path, DEFAULT_CONFIG)
    app.config.update(cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app(environ, start_response)


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0',
              help="Host to run the scheduler server api")
@click.option('--port', default=5008, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def launch(config_path, host, port, debug):
    app.config.update(config.read(config_path, DEFAULT_CONFIG))
    app.run(host, port=port, debug=bool(debug))


if __name__ == '__main__':
    launch()
