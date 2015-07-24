import six
from six.moves import configparser as CP
from sqlalchemy.engine.url import URL
import os


def parse(cell, config):
    parts = [part.strip() for part in cell.split(None, 1)]
    if not parts:
        return {'connection': '', 'sql': ''}
    if parts[0].startswith('[') and parts[0].endswith(']'):
        section = parts[0].lstrip('[').rstrip(']')
        parser = CP.ConfigParser()
        parser.read(os.path.expanduser(config.dsn_filename))
        cfg_dict = dict(parser.items(section))
        if 'uid' in cfg_dict:
            drivername = 'vertica' if 'vertica' in cfg_dict.get('driver', '') else 'unknown'
            connection = str(URL(
                drivername,
                username=cfg_dict.get('uid'),
                password=cfg_dict.get('pwd'),
                host=cfg_dict.get('servername'),
                port=cfg_dict.get('port'),
                database=cfg_dict.get('database')
            ))
        else:
            connection = str(URL(**cfg_dict))
        sql = parts[1] if len(parts) > 1 else ''
    elif '@' in parts[0] or '://' in parts[0]:
        connection = parts[0]
        if len(parts) > 1:
            sql = parts[1]
        else:
            sql = ''
    else:
        connection = ''
        sql = cell
    return {'connection': connection.strip(),
            'sql': sql.strip()}
