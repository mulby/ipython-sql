import sqlalchemy
from sqlalchemy.engine.url import make_url
from six import text_type

from sql.completer import completer


class Connection(object):
    current = None
    connections = {}
    @classmethod
    def tell_format(cls):
        return "Format: (postgresql|mysql|vertica)://username:password@hostname/dbname, or one of %s" \
               % str(cls.connections.keys())

    def __init__(self, connect_str=None):
        try:
            engine = sqlalchemy.create_engine(connect_str)
        except: # TODO: bare except; but what's an ArgumentError?
            print(self.tell_format())
            raise 
        self.metadata = sqlalchemy.MetaData(bind=engine)
        self.name = self.assign_name(engine.url)
        self.session = engine.connect() 
        self.connections[self.name] = self
        self.connections[str(self.metadata.bind.url)] = self
        Connection.current = self

    @classmethod
    def get(cls, descriptor):
        if isinstance(descriptor, Connection):
            cls.current = descriptor
        elif descriptor:
            conn = cls.connections.get(descriptor) or \
                   cls.connections.get(descriptor.lower()) 
            if conn:
                cls.current = conn
            else:
                if 'vertica' in descriptor:
                    cls.current = VerticaConnection(descriptor)
                else:
                    cls.current = Connection(descriptor)
        if cls.current:
            return cls.current
        else:
            raise Exception(cls.tell_format())

    @classmethod
    def assign_name(cls, url):
        core_name = '%s@%s' % (url.username, url.database)
        incrementer = 1
        name = core_name
        while name in cls.connections:
            name = '%s_%d' % (core_name, incrementer)
            incrementer += 1
        return name

    def execute(self, statement, user_namespace):
        txt = sqlalchemy.sql.text(statement)
        result = self.session.execute(txt, user_namespace)
        try:
            conn.session.execute('commit')
        except sqlalchemy.exc.OperationalError: 
            pass # not all engines can commit
        return result


class VerticaConnection(Connection):

    def __init__(self, connect_str=None):
        import vertica_python

        parsed_url = make_url(connect_str)
        params = parsed_url.translate_connect_args(
            host='host', username='user', password='password', port='port')
        self.internal_connection = vertica_python.connect(**params)
        self.cursor = self.internal_connection.cursor()
        self.name = self.assign_name(parsed_url)
        self.connections[self.name] = self
        self.connections[connect_str] = self
        Connection.current = self
        self.dialect = 'vertica'

        self.reset_completions()

    def execute(self, statement, user_namespace):
        from psycopg2 import ProgrammingError
        from psycopg2.extensions import adapt

        copied_namespace = {}
        for key, param in copied_namespace.items():
            try:
                if isinstance(param, text_type):
                    v = adapt(param.encode('utf8')).getquoted()
                else:
                    v = adapt(param).getquoted()
            except ProgrammingError:
                continue
            else:
                copied_namespace[key] = param

        self.cursor.execute(statement, copied_namespace)
        return VerticaResultProxy(self.cursor)

    def reset_completions(self):
        self.cursor.execute('show search_path')
        result = self.cursor.fetchone()
        search_path = [
            p.strip() for p in result[1].split(',')
        ]
        for i, entry in enumerate(search_path):
            if entry == "\"$user\"":
                search_path[i] = self.internal_connection.options.get('user')
        completer.set_search_path(search_path)

        self.cursor.execute('SELECT schema_name FROM v_catalog.schemata')
        result = self.cursor.fetchall()
        completer.extend_schemata([r[0] for r in result])

        self.cursor.execute('SELECT table_schema, table_name FROM v_catalog.tables')
        completer.extend_relations(self.cursor.fetchall(), kind='tables')

        self.cursor.execute('SELECT table_schema, table_name, column_name FROM v_catalog.columns')
        completer.extend_columns(self.cursor.fetchall(), kind='tables')

        self.cursor.execute('SELECT table_schema, table_name FROM v_catalog.views')
        completer.extend_relations(self.cursor.fetchall(), kind='views')

        self.cursor.execute('SELECT table_schema, table_name, column_name FROM v_catalog.view_columns')
        completer.extend_columns(self.cursor.fetchall(), kind='views')

        self.cursor.execute('SELECT projection_schema, projection_name FROM v_catalog.projections')
        completer.extend_relations(self.cursor.fetchall(), kind='views')

        self.cursor.execute(
            'SELECT p.projection_schema, p.projection_name, pc.projection_column_name FROM v_catalog.projection_columns pc '
            'JOIN v_catalog.projections p ON pc.projection_id = p.projection_id')
        completer.extend_columns(self.cursor.fetchall(), kind='views')


class VerticaResultProxy(object):

    def __init__(self, cursor):
        self.cursor = cursor
        self.returns_rows = True

    def keys(self):
        return [c.name for c in self.cursor.description]

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)
