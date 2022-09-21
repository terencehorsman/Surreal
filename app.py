import json
import requests
from flask import Flask, render_template, request
from datetime import timedelta


# configure app
app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=1)
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = 'secret'

# class for database connection
class SurrealDB:
    '''
    Class representing a surreal database

    Attributes
    ----------
        name (str): name of the database
        description (str): description of the database
        url (str): url of the api endpoint of the database
        namespace (str): namespace of the database
        database (str): database name
        auth (dict): authentication details {'username': 'username', 'password': 'password'}
        apis_enabled (dict): apis enabled {'info': True}
        database_tables (list): list of database tables

    Methods
    ----------
        _api_route(): returns the api route of the database
        _info(): returns the info of the database
        query(query): queries the database
        run_statements(statements (list)): runs statements on the database
        create_tables(): creates tables on the database
    '''
    
    def __init__(self, name: str, description: str, url: str, namespace: str, database: str, auth: dict, apis_enabled: dict = {'info': True}) -> None:
        self.name = name
        self.description = description
        self.url = url
        self.namespace = namespace
        self.database = database
        self.auth = auth
        self.apis_enabled = apis_enabled
        self.database_tables = []
        app.add_url_rule(self._api_route()+"/info", "db "+self.name+" info", lambda: self._info(), methods=['POST']) if self.apis_enabled['info'] else None

    def _api_route(self) -> str:
        return f"/api/v1/db/{self.name}"

    def _info(self) -> dict:
        return {
            'name': self.name,
            'description': self.description,
            'url': self.url,
            'namespace': self.namespace,
            'database': self.database,
            'api_route': self._api_route()
        }

    def query(self, query: str) -> str:
        output = ""
        try:
            headers = {'Content-Type': 'application/json', 'NS': self.namespace, 'DB': self.database}
            response = requests.post(self.url, headers = headers, data = query, auth=(self.auth.get('username'), self.auth.get('password')))
            output = json.dumps(response.json()[0])
        except Exception as e:
            output = {"error": "Error in query", "query": query, "exception": str(e)}
        return output

    def run_statements(self, statements: list) -> None:
        for statement in statements:
            try:
                self.query(statement)
            except:
                print("Error in statement: " + statement)
                pass    

    def create_tables(self) -> None:
        for table in self.database_tables:
            self.run_statements(table._create_table_statements())

# class for database tables
class SurrealDB_Table:
    '''
    Class to represent a database table, with methods to create, insert, select, update, and delete data
    upon creation, the table is added to the DATABASE_TABLES list

    Attributes
    ----------
        name: name of the table
        description: description of the table
        fields: dictionary of fields
        schemafull: boolean to indicate if the table is schemafull or schemaless (default: False)
        apis_enabled: dictionary of apis enabled for the table (default: {'read': True, 'write': True, 'delete': True, 'update': True, 'info': True})
    
    Methods
    -------
        _info: returns a dictionary of information about the table
        _api_route: returns the api route for the table
        read: read data from the table
        write: write data to the table
        delete: delete data from the table
        update: update data in the table
        _create_table_statements: returns a list of statements to create the table
        __insert_sql_statement: returns a list of statements to insert data into the table
        __select_sql_statement: returns a list of statements to select data from the table
        __delete_sql_statement: returns a list of statements to delete data from the table
        __update_sql_statement: returns a list of statements to update data in the table
        __validate_data: validates data to be inserted into the table

    '''
    def __init__(self, db: SurrealDB, name: str, description: str, fields: dict, schemafull: bool = False, apis_enabled: dict = {'read': True, 'write': True, 'delete': True, 'update': True, 'info': True}) -> None:
        self.name = name
        self.description = description
        self.fields = fields
        self.schemafull = schemafull
        self.apis_enabled = apis_enabled
        self.db = db
        db.database_tables.append(self)
        app.add_url_rule(self._api_route()+"/read", self.name+" read", (lambda: self.read(request.get_json())) if self.apis_enabled['read'] else (lambda: {"error": "API route not available"}), methods=['POST']) 
        app.add_url_rule(self._api_route()+"/write", self.name+" write", (lambda: self.write(request.get_json())) if self.apis_enabled['write'] else (lambda: {"error": "API route not available"}), methods=['POST'])
        app.add_url_rule(self._api_route()+"/delete", self.name+" delete", (lambda: self.delete(request.get_json())) if self.apis_enabled['delete'] else (lambda: {"error": "API route not available"}), methods=['POST'])
        app.add_url_rule(self._api_route()+"/update", self.name+" update", (lambda: self.update(request.get_json())) if self.apis_enabled['update'] else (lambda: {"error": "API route not available"}), methods=['POST'])
        app.add_url_rule(self._api_route()+"/info", self.name+" info", lambda: self._info(), methods=['POST']) if self.apis_enabled['info'] else None
    
    def __repr__(self) -> str:
        return f"{self.name} - {self.description} - fields: {self.fields.keys()}"
    
    def _api_route(self) -> str:
        return f"/api/v1/{self.name}"

    def _info(self) -> dict:
        return {
            'name': self.name,
            'description': self.description,
            'schemafull': self.schemafull,
            'api_route': self._api_route(),
            'apis_enabled': self.apis_enabled,
            'fields': self.fields
        }
    
    def _create_table_statements(self) -> list:
        table_definition = f"DEFINE TABLE {self.name} "
        table_definition += f"SCHEMAFULL" if self.schemafull else f"SCHEMALESS"
        table_definition += ";"
        statements = [table_definition]
        for field_name, field_properties in self.fields.items():
            statement = f"DEFINE FIELD {field_name} ON TABLE {self.name} TYPE {field_properties['type']} "
            statement += f"ASSERT {field_properties['assertion']} " if 'assertion' in field_properties else ""
            statement += f"VALUE {field_properties['value']} " if 'value' in field_properties else ""
            statement += ";"
            statements.append(statement)
        return statements
    
    def __select_sql_statement(self, data : dict = {}) -> list:
        statement = f"SELECT * FROM {self.name}"
        if data:
            statement += f":{data['id']} " if data['id'] else ""
        statements = [statement]
        return statements

    def __insert_sql_statement(self, data: dict = {}) -> list:
        fields = ", ".join(data.keys())
        values = ", ".join([f"'{value}'" for value in data.values()])
        statements = [f"INSERT INTO {self.name} ({fields}) VALUES ({values});"]
        return statements


    def __delete_sql_statement(self, data: dict = {}) -> list:
        key = list(data.keys())[0]
        value = data[key]
        statements = [f"DELETE {self.name}:{value};"] if key == 'id' else [""]
        return statements

    def __update_sql_statement(self, data: dict = {}) -> list:
        set_values = ", ".join([f"{key} = '{value}'" for key, value in data.items() if key != 'id'])
        id = data['id']
        statements = [f"UPDATE {self.name}:{id} SET {set_values};"]
        return statements
    
    def __validate_data(self, data: dict) -> bool:
        if data:
            if data['id']:
                return True
        return False

    def read(self, data: dict = {}) -> dict:
        return db.query(self.__select_sql_statement(data)[0])

    def write(self, data: dict) -> dict:
        return db.query(self.__insert_sql_statement(data)[0])

    def delete(self, data: dict) -> dict:
        if self.__validate_data(data):
            return db.query(self.__delete_sql_statement(data)[0])
        return {"error": "Invalid data, must contain an id"}
            
    def update(self, data: dict) -> dict:
        if self.__validate_data(data):
            return db.query(self.__update_sql_statement(data)[0])
        return {"error": "Invalid data, must contain an id"}


# initialise database
db = SurrealDB(
    name = "surreal",
    description = "SurrealDB",
    url = "http://localhost:8000/sql",
    namespace = "test",
    database = "test",
    auth = {
        'username': "root",
        'password': "root"
    },
    apis_enabled={
        'info': True
    }
)

# initialise tables
user = SurrealDB_Table(
    db = db,
    name= 'user', 
    description= 'User table', 
    schemafull=True, 
    fields={
        'name': {'type': 'object'},
        'name.first': {'type': 'string'},
        'name.last': {'type': 'string'}, 
        'name.full': {'type': 'string', 'value': 'name.first + " " + name.last'},
        'email': {'type': 'string', 'assertion': 'is::email($value)'}, 
        'password': {'type': 'string'}
    },
    apis_enabled={
        'read': True,
        'write': True,
        'delete': True,
        'update': True,
        'info': True
    }
)


# routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/test')
def test():
    user.write({'name.first': 'John', 'name.last': 'Doe', 'email': 'John.Doe@email.com', 'password': 'password'})
    return "done!"

# @app.route('/api/v1/db/create_indexes', methods=['GET'])
# def create_indexes():
#     # todo - create index method
#     return 'Indexes created'

if __name__ == '__main__':
    app.run()