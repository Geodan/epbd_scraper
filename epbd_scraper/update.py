# -*- coding: utf-8 -*-
"""

@author: Chris Lucas
"""

import argparse
import logging
import os
import sys
import datetime
import xml.sax
import psycopg2
from psycopg2.extensions import AsIs

from mutation.parse import EpbdContentHandler, EpbdErrorHandler, HigherError, LowerError, EqualError
from mutation.data import get_url, get_data


logger = logging.getLogger(__name__)


def parse_multiple_days(data, date, user, password, content_handler, error_handler, success=False):

    if data == {}:
        return

    year, month, day = [int(t) for t in date.split('-')]
    if success:
        day += 1
    else:
        if day > 1:
            day -= 1
        else:
            error_msg = 'No matching mutation files found. Completely refresh database using full EPBD XML file.'
            logger.error(error_msg)
            raise ValueError(error_msg)
    date = '{}-{:02}-{:02}'.format(year, month, day)

    if date in data:
        logger.info('Parsing mutation data of date: {} ..'.format(date))
        xml_data = data[date]
        xml.sax.parseString(xml_data, content_handler, error_handler)
        logger.info(
            'Parse complete. Data ({}) added to the database.'.format(date))
        data.pop(date)
        parse_multiple_days(data, date, user, password, content_handler,
                            error_handler, success=True)
    elif success:
        error_msg = 'Missing date in data.'
        logger.error(error_msg)
        raise ValueError(error_msg)
    else:
        logger.info(
            'Retrieving mutation data for date: {}, requesting url..'.format(date))
        url = get_url(date, user, password)
        logger.info('url retrieved: {}, downloading data..'.format(url))
        xml_data = get_data(url, date)
        logger.info('Download complete. Parsing data..')
        try:
            xml.sax.parseString(xml_data, content_handler, error_handler)
            logger.info(
                'Parse complete. Data ({}) added to the database.'.format(date))
            parse_multiple_days(data, date, user, password, content_handler,
                                error_handler, success=True)
        except HigherError:
            logger.info('Parse failed. '
                        'Latest Mutation number in database does not match mutation number of data.'
                        'Trying data from an earlier date..')
            data[date] = xml_data
            parse_multiple_days(data, date, user, password, content_handler,
                                error_handler, success=False)


def argument_parser():
    """
    Define and return the arguments.
    """
    description = ("Updates a EPBD postgresql database with daily mutations.")
    parser = argparse.ArgumentParser(description=description)
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
    required_named.add_argument('-pu', '--psqluser',
                                help='The username to access the PostgreSQL database.',
                                required=True)
    required_named.add_argument('-eu', '--epbduser',
                                help='The username to access the EPBD SOAP API.',
                                required=True)
    required_named.add_argument('-ep', '--epbdpassword',
                                help='The password to access the EPBD SOAP API.',
                                required=True)
    parser.add_argument('-pp', '--psqlpassword',
                        help='The password to access the PostgreSQL database.',
                        required=False,
                        default='')
    parser.add_argument('-p', '--port',
                        help='The port of the PostgreSQL database.',
                        type=int,
                        required=False,
                        default=5432)
    parser.add_argument('-a', '--date',
                        help='The date of the mutation xml file to be requested.',
                        required=False,
                        default=None)
    parser.add_argument('-l', '--logfile',
                        help='A path to a directory to save the logging information to.',
                        required=False,
                        default=None)
    parser.add_argument('-f', '--force',
                        help='Force the update without checking the mutation number. WARNING: Could lead to an invalid dataset.',
                        action='store_true')

    args = parser.parse_args()
    return args


def main():

    args = argument_parser()

    if args.date is None:
        date = str(datetime.date.today() - datetime.timedelta(days=1))
    else:
        date = args.date

    if args.logfile is not None:
        # log_dir = os.path.abspath(args.logdir)
        # log_file = '{}{}log_{}.txt'.format(log_dir, os.path.sep, date)
        logger.setLevel(logging.INFO)
        logging_handler = logging.FileHandler(args.logfile)
        logging_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging_handler.setFormatter(formatter)
        logger.addHandler(logging_handler)

    # TODO:
    # if int(date.split('-')[2]) == 1:
    #     logging.info('First day of the month, refreshing entire dataset..')

    logger.info(
        'Retrieving mutation data for date: {}, requesting url..'.format(date))

    try:
        url = get_url(date, args.epbduser, args.epbdpassword)
    except Exception as e:
        logger.exception("Error retrieving url")
        raise e

    logger.info('url retrieved: {}, downloading data..'.format(url))

    try:
        xml_data = get_data(url, date)
    except Exception as e:
        logger.exception("Error retrieving data")
        raise e

    logger.info('Download complete. Parsing data..')

    try:
        content_handler = EpbdContentHandler(args.host, args.dbname, args.schema,
                                             args.table, args.psqluser, args.psqlpassword,
                                             args.port, args.force)
        error_handler = EpbdErrorHandler()
    except Exception as e:
        logger.exception("Error setting up xml parser")
        raise e

    try:
        xml.sax.parseString(xml_data, content_handler, error_handler)
        logger.info(
            'Parse complete. Data ({}) added to the database.'.format(date))
    except HigherError:
        logger.info('Parse failed. '
                    'Latest Mutation number in database does not match mutation number of data.'
                    ' Trying data from an earlier date..')
        data = {date: xml_data}
        parse_multiple_days(data, date, args.epbduser, args.epbdpassword,
                            content_handler, error_handler)
    except LowerError:
        logger.error('Parse failed. '
                     'Data in database more recent than retrieved data.')
    except EqualError:
        logger.error('Parse failed. '
                     'Data in database already up to date with retrieved data.')


if __name__ == '__main__':
    main()
