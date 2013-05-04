MAX_NUM_POOL_WORKERS = 500
MAX_WAIT_TIME = 60000

DB_HOST = 'localhost'
DB_USER = ''
DB_PASSWD = ''
DB_NAME = 'eve'
DB_CONNECTIONS = 10

ZMQ_ADDRESS = ['tcp://relay-eu-france-2.eve-emdr.com:8050',
			   'tcp://relay-eu-germany-1.eve-emdr.com:8050',
               'tcp://relay-eu-france-1.eve-emdr.com:8050',
               'tcp://relay-us-west-1.eve-emdr.com:8050',
               'tcp://relay-us-central-1.eve-emdr.com:8050',
               'tcp://relay-us-east-1.eve-emdr.com:8050',
               'tcp://relay-eu-uk-1.eve-emdr.com:8050',
               'tcp://relay-eu-denmark-1.eve-emdr.com:8050']

REGION = {10000043: 'DOMAIN', 10000030: 'HEIMATAR', 10000016: 'LONETREK', 10000042: 'METROPOLIS',
          10000032: 'SINQ_LAISON', 10000002: 'THE_FORGE'}

SOLARSYSTEM = {30002187: 'Amarr', 30002510: 'Rens', 30001363: 'Sobaseki', 30002053: 'Hek', 30002659: 'Dodixie',
               30000142: 'Jita'}

STATION = {60008494: 'AmarrHUB', 60004588: 'RensHUB', 60003916: 'SobasekiHUB', 60005686: 'HekHUB',
           60011866: 'DodixieHUB', 60003760: 'JitaHUB'}