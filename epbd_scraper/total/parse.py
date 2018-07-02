# -*- coding: utf-8 -*-
"""
Parses EPBD XML data and puts it in a PostgreSQL database.

Based on epbdparser.py made by RVO.

@author: Chris Lucas
"""

import argparse
import xml.sax
import psycopg2
from psycopg2.extensions import AsIs


# -----------------------------------------------------------------------------
# EpbdErrorHandler
# -----------------------------------------------------------------------------
class EpbdErrorHandler(xml.sax.ErrorHandler):
    def error(self, exception):
        print(exception)

    def fatalError(self, exception):
        print(exception)


# -----------------------------------------------------------------------------
# EpbdContentHandler
# -----------------------------------------------------------------------------
class EpbdContentHandler(xml.sax.ContentHandler):
    def __init__(self, host, dbname, schema_name, table_name,
                 username, password='', port=5432, chunk_size=1000):
        self.Kolommen = {"Pand_postcode": "char(6)",
                         "Pand_huisnummer": "int",
                         "Pand_huisnummer_toev": "varchar(7)",
                         "Pand_bagverblijfsobjectid": "varchar(17)",
                         "Pand_opnamedatum": "date",
                         "Pand_berekingstype": "varchar(76)",
                         "Pand_energieprestatieindex": "real",
                         "Pand_energieklasse": "varchar(6)",
                         "Pand_registratiedatum": "date",
                         "Pand_energielabel_is_prive": "boolean",
                         "Meting_geldig_tot": "date",
                         "Pand_gebouwklasse": "char(1)",
                         "Pand_gebouwtype": "varchar(44)",
                         "Pand_gebouwsubtype": "varchar(19)",
                         "Pand_SBIcode": "int"}

        self.host = host
        self.dbname = dbname
        self.user = username
        self.password = password
        self.port = port
        self.schema_name = schema_name
        self.table_name = table_name
        self.chunk_size = chunk_size
        self.i = 0

    # -------------------------------------------------------------------------
    # aangeroepen bij de start van het document
    # -------------------------------------------------------------------------
    def startDocument(self):
        # Connect met de database
        conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(self.host,
                                                                                    self.dbname,
                                                                                    self.user,
                                                                                    self.password,
                                                                                    self.port)
        self.conn = psycopg2.connect(conn_str)
        self.cursor = self.conn.cursor()

        # als deze vlag waar wordt dan wordt data weg geschreven
        self.isdata = False
        self.isvolgnummer = False
        # deze waarde wordt gebruikt om te bepalen welk element nu verwerkt
        # wordt wordt gezet bij start element events, wordt gewist bij end
        # element events
        self.current = ""
        # gebruik een dictionary object als buffer aangezien sommige tags
        # niet altijd voorkomen
        self.data = {}
        for name in self.Kolommen:
            self.data[name] = ""

        # Creeer een tabel in de database
        query = "CREATE SCHEMA {}".format(AsIs(self.schema_name))
        self.cursor.execute(query)

        parameters = '(' + ','.join(['%s %s' for i in self.Kolommen]) + ')'
        query = "CREATE TABLE {}.{} {};".format(AsIs(self.schema_name),
                                                AsIs(self.table_name),
                                                parameters)
        columns = []
        for key, value in self.Kolommen.items():
            columns.append(key)
            columns.append(value)
        columns = [AsIs(x) for x in columns]
        self.cursor.execute(query, columns)

        query = "CREATE TABLE {}.laatste_volgnummer\
                 (volgnummer int);".format(AsIs(self.schema_name))
        self.cursor.execute(query)

        query = "INSERT INTO {}.laatste_volgnummer\
                 (volgnummer) VALUES (0);".format(AsIs(self.schema_name))
        self.cursor.execute(query)

    # -------------------------------------------------------------------------
    # aangeroepen bij de start van een nieuwe tag
    # -------------------------------------------------------------------------
    def startElement(self, name, attrs):

        if (name == "LaatstVerwerkteMutatieVolgnummer"):
            self.isvolgnummer = True
        elif (name in self.Kolommen):
            # alleen bij deze tags schrijven we data echt weg naar de csv file
            self.isdata = True
            self.current = name
        elif (name == "Pandcertificaat"):
            # begin van een nieuwe rij in het csv bestand
            # buffer opnieuw initialiseren
            self.buffer = ""

    # -------------------------------------------------------------------------
    # aangeroepen na lezen content van een tag
    # -------------------------------------------------------------------------
    def characters(self, content):
        # schrijf de waarde weg in de buffer indien het mag
        if (self.isdata):
            self.data[self.current] += content
        elif (self.isvolgnummer):
            self.volgnummer = content

    # -------------------------------------------------------------------------
    # aangeroepen bij het einde van een tag
    # -------------------------------------------------------------------------
    def endElement(self, name):
        if (name == "Pandcertificaat"):
            # Maak een query aan om de data in de database te zetten
            columns = "("
            parameters = "("
            values = []
            for key, value in self.data.items():
                if value != "":
                    columns += key + ", "
                    parameters += "%s" + ", "
                    values.append(value)
            columns = columns[:-2] + ")"
            parameters = parameters[:-2] + ")"

            query = "INSERT INTO {}.{} {} VALUES {};".format(AsIs(self.schema_name),
                                                             AsIs(
                                                                 self.table_name),
                                                             columns,
                                                             parameters)
            self.cursor.execute(query, values)

            # initialiseer de buffer opnieuw door alle waardes leeg te maken
            for name in self.data.keys():
                self.data[name] = ""
        elif (name == "LaatstVerwerkteMutatieVolgnummer"):
            query = "UPDATE {}.laatste_volgnummer\
                     SET volgnummer = %s;".format(AsIs(self.schema_name))
            self.cursor.execute(query, [self.volgnummer])

        # na sluiten van een tag altijd de current waarde leeg maken
        self.current = ""
        # na sluiten van een tag altijd de vlag voor wegschrijven van data
        # uitzetten
        self.isdata = False
        self.isvolgnummer = False

        self.i += 1
        if self.i == self.chunk_size:
            self.i = 0
            self.conn.commit()

    # -------------------------------------------------------------------------
    # aangeroepen bij het einde van het document
    # -------------------------------------------------------------------------
    def endDocument(self):
        # gebruik het einde van het document om de connectie met de database
        # te sluiten
        self.cursor.close()
        self.conn.commit()
        self.conn.close()


def argument_parser():
    """
    Define and return the arguments.
    """
    description = (
        "Reads an EPBD XML data file and writes it to a postgresql database.")
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('input_path', metavar='XMLFilePath',
                        help='The path to the EPBD XML file.')
    required_named = parser.add_argument_group('required named arguments')
    required_named.add_argument('-o', '--host',
                                help='The host adress of the PostgreSQL database.',
                                required=True)
    required_named.add_argument('-d', '--dbname',
                                help='The name of the database to write to.',
                                required=True)
    required_named.add_argument('-s', '--schema',
                                help='The name of the schema to write to.',
                                required=True)
    required_named.add_argument('-t', '--table',
                                help='The name of the table to write to.',
                                required=True)
    required_named.add_argument('-u', '--user',
                                help='The username to access the PostgreSQL database.',
                                required=True)
    parser.add_argument('-p', '--password',
                        help='The password to access the PostgreSQL database.',
                        required=False,
                        default='')
    parser.add_argument('-r', '--port',
                        help='The port of the PostgreSQL database.',
                        type=int,
                        required=False,
                        default=5432)
    parser.add_argument('-c', '--chunksize',
                        help='The numder of records in each commit.',
                        type=int,
                        required=False,
                        default=1000)

    args = parser.parse_args()
    return args


# -----------------------------------------------------------------------------
# start programma
# -----------------------------------------------------------------------------
def main():
    args = argument_parser()
    # parser object aanmaken
    parser = xml.sax.make_parser()
    # voeg objecten toe voor verwerking van de tags en error afhandeling
    parser.setContentHandler(EpbdContentHandler(args.host, args.dbname, args.schema,
                                                args.table, args.user, args.password,
                                                args.port, args.chunksize))
    parser.setErrorHandler(EpbdErrorHandler())
    # parse het bron bestand
    with open(args.input_path, "r") as f:
        src = xml.sax.xmlreader.InputSource()
        src.setByteStream(f)
        src.setEncoding("UTF-8")
        parser.parse(src)


if __name__ == '__main__':
    main()
