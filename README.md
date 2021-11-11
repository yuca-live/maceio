# MACEIÓ
Package to insert JSON data in SQL database.

## Features
- Create a new table based a json structure
- Insert a new line or update in table;

## How to use

```python
from Maceio.Maceio import Maceio

if __name__ == "__main__":
    # You can use only one dict withou list.
    data = '''[
            {
                "field1":{
                    "subfield1": 1,
                    "subfield2": "TYPE1",
                    "subfield3":{
                        "sub1":"Value 1",
                        "sub2":"Value 2",
                    }
                }
            },
            {
                "field1":{
                    "subfield1": 2,
                    "subfield2": "TYPE1",
                    "subfield3":{
                        "sub1":"Value 3",
                        "sub2":"Value 4",
                    }
                }
            }
        ]
    ''')

    maceio = Maceio('postgresql+psycopg2://user:password@host:port/database', 'schema_example')
    maceio.save('table_example', data, conflicts=('subfield1', 'subfield2'))
```