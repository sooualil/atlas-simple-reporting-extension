

import json
from time import time
from typing import Dict
from core.logger import Logger
from model.configuration.extension import ReportingConfig
from util.functions import loadConfig, relative

from core.database import __db__

try:
    # load and parse configuration
    config = loadConfig(ReportingConfig, relative(__file__, './config.yaml'))

    # Register extension channels into the current instance of the database manager
    __db__.register_extension(config)

    # Get logger
    logger = Logger(config.log, __name__.split('.')[-1])

    i = 0
    t = time()


    # Open log files
    log_file = open(relative(__file__, config.log.dir, 'log.txt'), 'w')

    # Iterated over subscribed channels
    for data in __db__.get_message():
        # check if is is a message from the publisher
        if data['type'] == 'message':
            # Log received inference
            # logger.debug('Received inference', data['data'], 'and logging to', f'{config.log.dir}/log.txt')

            if time() - t > 3: 
                logger.debug(f'{i} items processed')
                t = time()
            i+=1


            # Get inference from database
            inference = __db__.retrieve('inference', data['data'])

            line = '\n'.join([f'{k}\t{v}' for k, v in inference['predictions'].items()])

            # for k in inference['predictions']:
            #     __db__.delete('flow', k)
            #     __db__.delete('inference', data['data'])

            # Write in log
            log_file.write(line + '\n')


except KeyboardInterrupt:
    log_file.close()
    print('aight see ya')