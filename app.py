import json
import requests
from flask import Flask, render_template, request
from datetime import timedelta


# configure app
app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = timedelta(seconds=1)
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = 'secret'
app.config['SURREALDB'] = {
    'url': 'http://localhost:8000/sql',
    'NS': 'test',
    'DB': 'test',
    'auth': {
        'username': 'root',
        'password': 'root'
    }
}


# static list of database tables
DATABASE_TABLES = []


# utility function to query surrealdb
def query_surrealdb(query):
    output = ""
    try:
        url = app.config['SURREALDB'].get('url')
        headers = {'Content-Type': 'application/json', 'NS': app.config['SURREALDB'].get('NS'), 'DB': app.config['SURREALDB'].get('DB')}
        response = requests.post(url, headers = headers, data = query, auth=(app.config['SURREALDB'].get('auth').get('username'), app.config['SURREALDB'].get('auth').get('password')))
        output = json.dumps(response.json()[0])
    except Exception as e:
        output = {"error": "Error in query", "query": query, "response": response.text, "exception": str(e)}
    return output

# utility function to run statements
def run_statements(statements):
    for statement in statements:
        try:
            query_surrealdb(statement)
        except:
            print("Error in statement: " + statement)
            pass


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
    
    Methods
    -------
        api_route: returns the api route for the table
        create_table_statements: returns a list of statements to create the table
        insert_sql_statement: returns a list of statements to insert data into the table
        select_sql_statement: returns a list of statements to select data from the table
        delete_sql_statement: returns a list of statements to delete data from the table
        update_sql_statement: returns a list of statements to update data in the table

    '''
    def __init__(self, name: str, description: str, fields: dict, schemafull: bool = False):
        self.name = name
        self.description = description
        self.fields = fields
        self.schemafull = schemafull
        DATABASE_TABLES.append(self)
    
    def __repr__(self):
        return f"{self.name} - {self.description} - fields: {self.fields.keys()}"
    
    def api_route(self) -> str:
        return f"/api/v1/{self.name}"
    
    def create_table_statements(self) -> list:
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
    
    def insert_sql_statement(self, data: dict) -> list:
        fields = ", ".join(data.keys())
        values = ", ".join([f"'{value}'" for value in data.values()])
        statements = [f"INSERT INTO {self.name} ({fields}) VALUES ({values});"]
        return statements

    def select_sql_statement(self) -> list:
        statements = [f"SELECT * FROM {self.name}"]
        return statements

    def delete_sql_statement(self, data: dict) -> list:
        key = list(data.keys())[0]
        value = data[key]
        statements = [f"DELETE {self.name}:{value};"] if key == 'id' else [""]
        return statements

    def update_sql_statement(self, data: dict) -> list:
        set_values = ", ".join([f"{key} = '{value}'" for key, value in data.items() if key != 'id'])
        id = data['id']
        statements = [f"UPDATE {self.name}:{id} SET {set_values};"]
        return statements


# initialise database tables
user = SurrealDB_Table(
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
    }
)


# routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/v1/db/read', methods=['POST'])
def api_surrealdb():
    data = request.get_json()
    query = data['query']
    return query_surrealdb(query)

@app.route('/api/v1/db/create_tables', methods=['GET'])
def create_tables():
    for table in DATABASE_TABLES:
        run_statements(table.create_table_statements())
    return 'Tables created'

# create api routes for each table
for table in DATABASE_TABLES:
    app.add_url_rule(table.api_route()+"/read", table.name+" read", lambda: query_surrealdb(table.select_sql_statement()[0]), methods=['GET'])
    app.add_url_rule(table.api_route()+"/write", table.name+" write", lambda: query_surrealdb(table.insert_sql_statement(request.get_json())[0]), methods=['POST'])
    app.add_url_rule(table.api_route()+"/delete", table.name+" delete", lambda: query_surrealdb(table.delete_sql_statement(request.get_json())[0]), methods=['POST'])
    app.add_url_rule(table.api_route()+"/update", table.name+" update", lambda: query_surrealdb(table.update_sql_statement(request.get_json())[0]), methods=['POST'])


if __name__ == '__main__':
    app.run()