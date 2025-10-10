

import logging
import random

def generate_log():
    '''generate fake logs for tesing'''

    logging.basicConfig(level=logging.INFO,filename="logs\\fake_log_gen.log",
                        format='%(asctime)s :%(levelname)s: %(message)s',datefmt='%Y-%m-%d %H:%M:%S')


    names=['John','Jane','Bob','Alice','Mike','Sara','Tom','Lily','Mike','Sara','Tom','Lily']
    actions=['login','logout','click','scroll','refresh']
    domains=['gmail.com','yahoo.com','hotmail.com','outlook.com']

    for _ in range(100):
        name=random.choice(names)
        action=random.choice(actions)
        domain=random.choice(domains)
        
        log=f'{name}-{action}-{domain}'

        logging.info(log)