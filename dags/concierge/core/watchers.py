# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
INTERVAL_MINUTES = 60


class LastSeqNumber(object):

    _number = 0
    _drop_date = None

    def set(self, num):
        self._number = num
        if self._drop_date is None:
            self._drop_date = datetime.now() + timedelta(minutes=INTERVAL_MINUTES)

    def get(self):
        return self._number

    def drop(self, logger):
        if self._drop_date and datetime.now() > self._drop_date:
            self._number = 0
            self._drop_date = datetime.now() + timedelta(minutes=INTERVAL_MINUTES)
            logger.info('Drop last_seq. Full database will be filtered')
