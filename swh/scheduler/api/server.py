# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os

from swh.core import config
from swh.core.api import JSONFormatter, MsgpackFormatter, RPCServerApp
from swh.core.api import encode_data_server as encode_data
from swh.core.api import error_handler, negotiate
from swh.scheduler import get_scheduler
from swh.scheduler.exc import SchedulerException
from swh.scheduler.interface import SchedulerInterface

from .serializers import DECODERS, ENCODERS

scheduler = None


def get_global_scheduler():
    global scheduler
    if not scheduler:
        scheduler = get_scheduler(**app.config["scheduler"])
    return scheduler


class SchedulerServerApp(RPCServerApp):
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS


app = SchedulerServerApp(
    __name__, backend_class=SchedulerInterface, backend_factory=get_global_scheduler
)


@app.errorhandler(SchedulerException)
def argument_error_handler(exception):
    return error_handler(exception, encode_data, status_code=400)


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


def has_no_empty_params(rule):
    return len(rule.defaults or ()) >= len(rule.arguments or ())


@app.route("/")
def index():
    return """<html>
<head><title>Software Heritage scheduler RPC server</title></head>
<body>
<p>You have reached the
<a href="https://www.softwareheritage.org/">Software Heritage</a>
scheduler RPC server.<br />
See its
<a href="https://docs.softwareheritage.org/devel/swh-scheduler/">documentation
and API</a> for more information</p>
</body>
</html>"""


@app.route("/site-map")
@negotiate(MsgpackFormatter)
@negotiate(JSONFormatter)
def site_map():
    links = []
    for rule in app.url_map.iter_rules():
        if has_no_empty_params(rule) and hasattr(SchedulerInterface, rule.endpoint):
            links.append(
                dict(
                    rule=rule.rule,
                    description=getattr(SchedulerInterface, rule.endpoint).__doc__,
                )
            )
    # links is now a list of url, endpoint tuples
    return links


def load_and_check_config(config_path, type="local"):
    """Check the minimal configuration is set to run the api or raise an
       error explanation.

    Args:
        config_path (str): Path to the configuration file to load
        type (str): configuration type. For 'local' type, more
                    checks are done.

    Raises:
        Error if the setup is not as expected

    Returns:
        configuration as a dict

    """
    if not config_path:
        raise EnvironmentError("Configuration file must be defined")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} does not exist")

    cfg = config.read(config_path)

    vcfg = cfg.get("scheduler")
    if not vcfg:
        raise KeyError("Missing '%scheduler' configuration")

    if type == "local":
        cls = vcfg.get("cls")
        if cls != "local":
            raise ValueError(
                "The scheduler backend can only be started with a 'local' "
                "configuration"
            )

        db = vcfg.get("db")
        if not db:
            raise KeyError("Invalid configuration; missing 'db' config entry")

    return cfg


api_cfg = None


def make_app_from_configfile():
    """Run the WSGI app from the webserver, loading the configuration from
       a configuration file.

       SWH_CONFIG_FILENAME environment variable defines the
       configuration path to load.

    """
    global api_cfg
    if not api_cfg:
        config_path = os.environ.get("SWH_CONFIG_FILENAME")
        api_cfg = load_and_check_config(config_path)
        app.config.update(api_cfg)
    handler = logging.StreamHandler()
    app.logger.addHandler(handler)
    return app


if __name__ == "__main__":
    print('Please use the "swh-scheduler api-server" command')
