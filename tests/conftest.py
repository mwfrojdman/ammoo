import os
from functools import partial

import pytest

from ammoo import connect


@pytest.fixture(scope='function')
def rabbitmq_host():
    return os.environ['AMMOO_TEST_AMQP_HOST']


@pytest.fixture(scope='function')
def rabbitmq_virtualhost():
    return os.environ['AMMOO_TEST_VIRTUALHOST']


# TODO: refactor most (all?) tests to use this fixture so they get the correct virtualhost too
@pytest.fixture(scope='function')
def connect_to_broker(rabbitmq_host, rabbitmq_virtualhost, event_loop):
    return partial(connect, host=rabbitmq_host, virtualhost=rabbitmq_virtualhost, loop=event_loop)
