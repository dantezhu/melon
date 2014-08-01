# -*- coding: utf-8 -*-

#from netkit.box import Box as Box
from kola_box import KolaBox as Box

import logging
from melon import logger, Melon, Blueprint

logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)