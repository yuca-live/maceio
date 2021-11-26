from sqlalchemy import create_engine, MetaData, Table, Column, DateTime, BigInteger, Text, Integer, Float, Boolean, UniqueConstraint, exc
from sqlalchemy.dialects.postgresql import JSON, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import json
import unidecode
import re

class Maceio():
    """
    Maceió
    This is the Maceio class. Focused on building SQL tables based on a JSON format. The name was given in honor of Yuca's first property, the company where the lib was developed by the Data team.
    """

    def __init__(self, engineText, schema, pool_size=5, max_overflow=2, echo=False, enableJsonType=True, onlyText=False):
        """
        Constructor method

        :engineText: Database connection URI. For now we have 100% support for PostgreSQL, other databases may not perform well when there is conflict in inserts and for JSON type fields.
        :schema: Name of the schema that will be used.
        :pool_size: Size of max connections that can be use.
        :max_overflow: Limit of max connections that can exceded.
        :echo: Parameter that will display a sql log in the terminal, if True.
        :enableJsonType: Default True - Enable Json types in create table (Only to PostgreSQL).
        :onlyText: Disable all types and convert all to string (Used to inconsistant datas).
        :return: void
        """
        self.schema = schema
        self.engineText = engineText
        self.meta = MetaData(schema=self.schema)
        self.engine = create_engine(engineText, pool_size=pool_size, max_overflow=max_overflow, echo=echo)
        self.base = declarative_base()
        self.enableJsonType = enableJsonType
        self.onlyText = onlyText

        self.__type = {
            'str': Text,
            'int': Integer if not onlyText else Text,
            'float': Float if not onlyText else Text,
            'bool': Boolean if not onlyText else Text,
            'NoneType': Text,
            'dict': JSON if 'postgresql' in self.engineText and enableJsonType and not onlyText else Text,
            'tuple': JSON if 'postgresql' in self.engineText and enableJsonType and not onlyText else Text,
            'list': JSON if 'postgresql' in self.engineText and enableJsonType and not onlyText else Text
        }

        self.__sqlType = {
            'str': 'text' if not onlyText else 'text',
            'int': 'int4' if not onlyText else 'text',
            'float': 'float8' if not onlyText else 'text',
            'bool': 'bool' if 'postgresql' in self.engineText else ('int' if not onlyText else 'text'),
            'NoneType': 'text',
            'dict': 'json' if 'postgresql' in self.engineText and enableJsonType and not onlyText else 'text',
            'tuple': 'json' if 'postgresql' in self.engineText and enableJsonType and not onlyText else 'text',
            'list': 'json' if 'postgresql' in self.engineText and enableJsonType and not onlyText else 'text'
        }

    def save(self, table, data, conflicts=[], verify=True, primaries=[]):
        """
        Method that saves data to the database.

        :table: Database table name.
        :data: String with a json inside or a dictionary.
        :conflicts: Tuple with conflicting field names (Used for updating existing lines).
        :verify: Boolean to enable or disable verify to create a table (Disable when the table exists).
        primaries: Tuple with primary key field names.
        :return: void
        """
        if isinstance(data, str):
            data = json.loads(data)

        columns, nameCols, elements = [], {}, []

        if isinstance(data, list):
            if len(data) == 0:
                raise Exception('Não há valores a serem inseridos.')

            for item in data:
                cols, cNameCols, element = self.__generateColumnsAndData(item, primaries=primaries, extract_updatet_at=True)

                columns.extend(cols)
                nameCols.update(cNameCols)
                elements.append(element)
        else:
            if len(data.keys()) == 0:
                raise Exception('Não há valores a serem inseridos.')

            columns, nameCols, element = self.__generateColumnsAndData(data, primaries=primaries, extract_updatet_at=True)
            elements.append(element)
        
        elements = self.__fillNaNColumns(nameCols, elements)

        name_index_unique = None

        self.__verifyTableUpdates(table, nameCols)
        table, name_index_unique = self.__addTable(table, columns, conflicts, verify, primaries)

        self.__insert(table, elements, nameCols, name_index_unique)
        self.engine.dispose()

    def __verifyTableUpdates(self, table, nameCols):
        """
        Method to check new keys in a json of an existing table and create alter table from it.

        :table: Name of table to update.
        :nameCols: List with column names.
        :return: void.
        """

        try:
            conn = self.engine.connect()
            results = conn.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '{self.schema}' AND table_name = '{table}';")

            results = set([r['column_name'] for r in results])
            if len(results) > 0:
                colsSet = set(nameCols)
                updates = list(colsSet - results)

                for update in updates:
                    try:
                        self.__alterTable(f'ALTER TABLE {self.schema}.{table} ADD {update} {self.__sqlType[nameCols[update]]};')
                    except Exception as e:
                        print(f"Erro ao atualizar table. {e}")
        except Exception as e:
            print(f"Um erro ocorreu ao tentar atualizar a base do dado. {e}")
        finally:
            conn.close()

    def __alterTable(self, query):
        """
        Method to alter table adding a new column her.

        :query: Query to update table.
        :return: void.
        """

        with self.engine.connect() as connection:
            with connection.begin():    
                connection.execute(query)

    def __fillNaNColumns(self, nameCols, elements):
        """
        Method to fill elements that don't have a key with this keys with none value.

        :nameCols: Dictionary with keys with column names.
        :elements: Dictionary with values to insert in database.
        :return: dict.
        """

        for i, e in enumerate(elements):
            keys = e.keys()
            for name in nameCols.keys():
                if name not in keys:
                    elements[i].update({name: None})

        return elements
        
    def __insert(self, table, data, nameCols, name_index_unique=None):
        """
        Method that inserts or updates data in the database.

        :table: Object that generated the table.
        :data: Tuple containing the data to be entered.
        :name_index_unique: List of uniques
        :return: void
        """
        try:
            conn = self.engine.connect()
            try:
                insert_stmt = insert(table).values(data)

                updateFields = self.__generateUpdateConflicts(nameCols, insert_stmt)
                do_update_stmt = insert_stmt.on_conflict_do_update(
                    constraint=name_index_unique,
                    set_=updateFields
                )
                conn.execute(do_update_stmt)
                print("Inserido")
            except Exception as e:
                import ipdb; ipdb.set_trace()
                print(f"Um erro ocorreu ao tentar inserir o dado. {e}")
        except Exception as e:
            print(f"Um erro ocorreu. {e}")
        finally:
            conn.close()

    def __generateUpdateConflicts(self, nameCols, insert_stmt):
        """
        Method that creates the dictionary that will have all fields to be updated in case there is a conflict in the insert.

        :data: Dictionary with insert data. Dictionary position 1 only.
        :nameCols: Dictionary with keys with column names.
        :insert_stmt: Insert instance that is needed in sqlalchemy to create the excluded.
        :return: dict
        """
        updateFields = {}

        for name in nameCols.keys():
            updateFields.update({name: insert_stmt.excluded[name]})

        return updateFields

    def __addTable(self, table, columns, conflicts, verify=True, primaries=[]):
        """
        Method that creates a pivot table in the database.

        :table: Database table name.
        :columns: A tuple containing all columns in sqlalchemy format.
        :conflicts: List with name of string conflicts.
        :verify: Boolean to verify if table exist.
        :primaries: List with primary keys.
        :return: object, string.
        """
        columns += (Column('extracted_at', DateTime(timezone=True), server_default=func.now()),
                    Column('extract_updatet_at', DateTime(timezone=True), onupdate=func.now()))

        name_index_unique = None
        if len(conflicts) > 0:
            name_index_unique = f'uix_{table}_{"_".join(conflicts)}'
            columns += (UniqueConstraint(*conflicts, name=f'{name_index_unique}'),)

        try:
            if primaries:
                table = Table(table, self.meta, *columns, extend_existing=True,)
            else:
                table = Table(table, self.meta, Column('id', BigInteger, primary_key=True), *columns, extend_existing=True,)
        except Exception as e:
            print(f'3 - {e}')

        if verify:
            self.meta.create_all(self.engine, checkfirst=True)

        return table, name_index_unique

    def __testJson(self, value):
        """
        Method the verify if is a possible json.

        :value: String to verify if is possible to be a json.
        :return: bool.
        """

        try:
            if('{' in value and '}' in value):
                json.loads(value)
                return True
            return False
        except:
            return False

    def __generateColumnsAndData(self, data, nodes=3, level=1, parentName=None, primaries=None, extract_updatet_at=False):
        """
        Method that generates the columns that will be created in the database.

        :data: Dictionary that contains the data that must be entered.
        :nodes: Max limit to call method recursive'
        :level: Current call this recursive method is current.
        :parentName: The parent key to this current key.
        :primaries: Key primary values in table.
        :extract_updatet_at: Value to enable or disable current value to extract_updatet_at field.
        :return: tuple.
        """

        columns, nameCols, dataDict = [], {}, {}
        for e in data.items():
            if level == 1:
                name = e[0]
            else:
                name = f'{parentName}_{e[0]}'

            name = name.lower()
            name = unidecode.unidecode(name)
            name = re.sub(r"[^\w\s]", '_', name)
            name = re.sub(r"\s+", '_', name)

            if type(e[1]).__name__ == 'dict' or self.__testJson(e[1]):
                if level <= nodes:
                    if self.__testJson(e[1]):
                        columns.append(Column(name, self.__type['dict'], primary_key=name in primaries))
                        nameCols.update({name: 'dict'})

                        dataDict = self.__saveDictJson(dataDict, name, e)
                    else:
                        cols, cNameCols, datas = self.__generateColumnsAndData(e[1], nodes, level + 1, name, primaries)
                        
                        columns.extend(cols)
                        nameCols.update(cNameCols)
                        dataDict.update(datas)
                else:
                    columns.append(Column(name, self.__type['dict'], primary_key=name in primaries))
                    nameCols.update({name: 'dict'})
                    dataDict = self.__saveDictJson(dataDict, name, e)
            else:
                columns.append(Column(name, self.__type[type(e[1]).__name__], primary_key=name in primaries))
                nameCols.update({name: type(e[1]).__name__})
                
                value = e[1]
                if (type(value).__name__ == 'list' and 'postgresql' not in self.engineText) or (type(value).__name__ == 'list' and not self.enableJsonType) or (type(value).__name__ == 'list' and self.onlyText):
                    value = json.dumps(value)
                elif self.onlyText:
                    value = str(value)
                    if value == 'True':
                        value = '1'
                    elif value == 'False':
                        value = '0'

                dataDict.update({name: value})

        if extract_updatet_at:
            dataDict.update({'extract_updatet_at': func.now()})

        return columns, nameCols, dataDict

    def __saveDictJson(self, dataDict, name, e):
        """
        Method to update a dict data with new value

        :dataDict: Dictionary that will be update.
        :name: Name of new key that dictionary will receive.
        :e: Element with two positions, pos 1 with name and pos 2 with value.
        :return: dict.
        """

        if self.enableJsonType and not self.onlyText:
            dataDict.update({name: json.loads(e[1])})
        else:
            if not isinstance(e[1], str):
                dataDict.update({name: json.dumps(e[1])})
            else:
                dataDict.update({name: e[1]})
        return dataDict