from sqlalchemy import create_engine, MetaData, Table, Column, DateTime, BigInteger, Text, Integer, Float, Boolean, UniqueConstraint, exc
from sqlalchemy.dialects.postgresql import JSON, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import json


class Maceio():
    """
    Maceió
    This is the Maceio class. Focused on building SQL tables based on a JSON format. The name was given in honor of Yuca's first property, the company where the lib was developed by the Data team.
    """

    def __init__(self, engineText, schema, echo=False):
        """
        Constructor method

        :engineText: Database connection URI. For now we have 100% support for PostgreSQL, other databases may not perform well when there is conflict in inserts and for JSON type fields.
        :schema: Name of the schema that will be used.
        :echo: Parameter that will display a sql log in the terminal, if True.
        :return: void
        """

        self.meta = MetaData(schema=schema)
        self.engine = create_engine(engineText, echo=echo)
        self.base = declarative_base()

        self.__type = {
            'str': Text,
            'int': Integer,
            'float': Float,
            'bool': Boolean,
            'dict': JSON if 'postgresql' in engineText else Text,
            'tuple': Text,
            'list': Text,
        }

    def save(self, table, data, conflicts=(), verify=True):
        """
        Method that saves data to the database.

        :table: Database table name.
        :data: String with a json inside or a dictionary.
        :return: void
        """
        if isinstance(data, str):
            data = json.loads(data)

        columns, elements = [], []

        if isinstance(data, list):
            for item in data:
                columns, element = self.__generateColumnsAndData(item)
                elements.append(element)
        else:
            columns, element = self.__generateColumnsAndData(data)
            elements.append(element)

        try:
            table, name_index_unique = self.__addTable(table, columns, conflicts, verify)
        except Exception as e:
            print(f"6 - {e}")

        self.__insert(table, elements, name_index_unique)

    def __insert(self, table, data, name_index_unique):
        """
        Method that inserts or updates data in the database.

        :table: Object that generated the table.
        :data: Tuple containing the data to be entered.
        :return: void
        """
        try:
            conn = self.engine.connect()
            insert_stmt = insert(table).values(data)

            updateFields = self.__generateUpdateConflicts(data[0], insert_stmt)
            do_update_stmt = insert_stmt.on_conflict_do_update(
                constraint=name_index_unique,
                set_=updateFields
            )
            conn.execute(do_update_stmt)
        except Exception as e:
            print(f"Um erro ocorreu. {e}")

    def __generateUpdateConflicts(self, data, insert_stmt):
        """
        Method that creates the dictionary that will have all fields to be updated in case there is a conflict in the insert.

        :data: Dictionary with insert data. Dictionary position 1 only.
        :insert_stmt: Insert instance that is needed in sqlalchemy to create the excluded.
        :return: dict
        """
        updateFields = {}

        for key in data.keys():
            updateFields.update({key: insert_stmt.excluded[key]})

        return updateFields

    def __addTable(self, table, columns, conflicts, verify=True):
        """
        Method that creates a pivot table in the database.

        :table: Database table name.
        :columns: A tuple containing all columns in sqlalchemy format.
        :return: object
        """
        columns += (Column('extracted_at', DateTime(timezone=True), server_default=func.now()),
                    Column('extract_updatet_at', DateTime(timezone=True), onupdate=func.now()))

        name_index_unique = None
        if len(conflicts) > 0:
            name_index_unique = f'uix_{table}_{"_".join(conflicts)}'
            columns += (UniqueConstraint(*conflicts,
                        name=f'{name_index_unique}'),)

        try:
            table = Table(table, self.meta, Column('id', BigInteger, primary_key=True), *columns)
        except Exception as e:
            print(f'3 - {e}')

        if verify:
            self.meta.create_all(self.engine, checkfirst=True)

        return table, name_index_unique

    def __testJson(self, value):
        try:
            if(('{' in value and '}' in value) or ('[' in value and ']' in value)):
                json.loads(value)
                return True
            return False
        except:
            return False

    def __generateColumnsAndData(self, data, nodes=3, level=1, parentName=None):
        """
        Method that generates the columns that will be created in the database.

        :data: Dictionary that contains the data that must be entered.
        :return: tuple
        """
        columns, dataDict = [], {}
        for e in data.items():
            if level == 1:
                name = e[0]
            else:
                name = f'{parentName}_{e[0]}'

            if type(e[1]).__name__ == 'dict' or self.__testJson(e[1]):
                if level <= nodes:
                    if self.__testJson(e[1]):
                        columns.append(Column(e[0], self.__type['dict']))
                        dataDict.update({name: json.loads(e[1])})
                    else:
                        cols, datas = self.__generateColumnsAndData(e[1], nodes, level + 1, name)
                        columns += cols     

                        dataDict.update(datas)
                else:
                    columns.append(Column(e[0], self.__type['dict']))
                    dataDict.update({name: json.loads(e[1])})
            else:
                columns.append(Column(name, self.__type[type(e[1]).__name__]))     
                dataDict.update({name: e[1]})

        return tuple(columns), dataDict