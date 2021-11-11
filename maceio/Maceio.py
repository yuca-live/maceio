from sqlalchemy import create_engine, MetaData, Table, Column, DateTime, BigInteger, Text, Integer, Float, Boolean, UniqueConstraint, exc
from sqlalchemy.dialects.postgresql import JSON, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import json

class Maceio():
    """
    Maceió
    Está é a classe Maceió. Focada em construir tabelas SQL baseadas em um formato JSON. O nome dado foi em homenagem ao primeiro imóvel da Yuca, empresa onde a lib foi desenvolvida pelo time de Data.
    """


    def __init__(self, engineText, schema, echo=False):
        """
        Método construtor

        :engineText: URI de conexão ao banco. Por enquanto temos suporte 100% ao PostgreSQL, outros bancos podem não performar bem quando houver conflito em inserções e para campos do tipo JSON.
        :schema: Nome do schema que será usado.
        :echo: Parâmetro que exibirá um log do sql no terminal, caso seja passado True.
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
            'dict': JSON if 'psql' in engineText else Text,
            'tuple': Text,
            'list': Text,
        }

    def save(self, table, data, conflicts=(), verify=True):
        """
        Método que salva os dados no banco

        :table: nome da tabela do banco de dados
        :data: dicionário que contém os dados que devem ser inseridos.
        :return: void
        """
        elements = []

        if isinstance(data, list):
            for item in data:
                columns, element = self.__generateColumnsAndData(item)
                elements.append(element)
        else:
            columns, element = self.__generateColumnsAndData(data)
            elements.append(element)

        table, name_index_unique = self.__addTable(table, columns, conflicts, verify)
        self.__insert(table, elements, name_index_unique)

    def __insert(self, table, data, name_index_unique):
        """
        Método que insere ou atualiza dados na base.

        :table: Objeto que gerou a tabela
        :data: tupla que contém os dados que devem ser inseridos.
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
        Método que cria o dicionário que terá todos os campos para serem atualizados caso haja conflito no insert.

        :data: Dicionário com os dados de insert. Apenas a posição 1 do dicionário
        :insert_stmt: Instância do insert que é necessária no sqlalchemy para criarmos o excluded
        :return: dict
        """
        updateFields = {}

        for key in data.keys():
            updateFields.update({key: insert_stmt.excluded[key]})
        
        return updateFields

    def __addTable(self, table, columns, conflicts, verify=True):
        """
        Método que cria uma tabela dinâmica no banco

        :table: nome da tabela do banco de dados
        :columns: uma tupla contendo todas as colunas no formato do sqlalchemy
        :return: object
        """

        columns += (Column('extracted_at', DateTime(timezone=True), server_default=func.now()), Column('extract_updatet_at', DateTime(timezone=True), onupdate=func.now()))

        name_index_unique = None
        if len(conflicts) > 0:
            name_index_unique = f'uix_{table}_{"_".join(conflicts)}'
            columns += (UniqueConstraint(*conflicts, name=f'{name_index_unique}'),)

        table = Table(table, self.meta, Column('id', BigInteger, primary_key=True), *columns)

        if verify:
            self.meta.create_all(self.engine, checkfirst=True)
        
        return table, name_index_unique

    def __generateColumnsAndData(self, data, nodes=3, level=1, parentName=None):
        """
        Método que gera as colunas que serão criadas no banco de dados.

        :data: dicionário que contém os dados que devem ser inseridos.
        :return: tuple
        """
        columns, dataDict = [], {}
        for e in data.items():
            if level == 1:
                name = e[0]
            else:
                name = f'{parentName}_{e[0]}'

            if type(e[1]).__name__ == 'dict':
                if level <= nodes:
                    cols, datas = self.__generateColumnsAndData(e[1], nodes, level + 1, name)
                    columns += cols
                    dataDict.update(datas)
                else:
                    columns.append(Column(e[0], self.__type['str']))
                    dataDict.update({name:json.dumps(e[1])})
            else:
                columns.append(Column(name, self.__type[type(e[1]).__name__]))
                dataDict.update({name: e[1]})

        return tuple(columns), dataDict