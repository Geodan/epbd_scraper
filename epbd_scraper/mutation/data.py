# -*- coding: utf-8 -*-
"""

@author: Chris Lucas
"""

import os
import logging
import xml.etree.ElementTree
import zipfile
from io import BytesIO
import requests


logger = logging.getLogger(__name__)


def get_url(date, username, password):
    # request
    headers = {'content-type': 'text/xml',
               'SOAPAction': 'http://schemas.ep-online.nl/EpbdDownloadMutationFileService/DownloadMutationFile'}

    req = """<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Header>
        <EpbdDownloadMutationFileHeader xmlns="http://schemas.ep-online.nl/EpbdDownloadMutationFileHeader">
        <username>{}</username>
        <password>{}</password>
        </EpbdDownloadMutationFileHeader>
    </soap:Header>
    <soap:Body>
        <DownloadMutationFile xmlns="http://schemas.ep-online.nl/EpbdDownloadMutationFileService">
        <request>
            <mutationType>Mutation</mutationType>
            <date>{}</date>
        </request>
        </DownloadMutationFile>
    </soap:Body>
    </soap:Envelope>""".format(username, password, date)

    r = requests.post("https://webapplicaties.agro.nl/DownloadMutationFile/EPBDDownloadMutationFile.asmx",
                      data=req, headers=headers)

    # response

    tree = xml.etree.ElementTree.fromstring(r.content)
    element = tree.find(
        './/{http://schemas.ep-online.nl/EpbdDownloadMutationFileResponse}downloadURL')
    url = element.text

    return url


def get_data(url, date):
    r = requests.get(url)
    response_data = BytesIO(r.content)
    zipped_data = zipfile.ZipFile(response_data)
    try:
        name = 'd{}.xml'.format(date.replace('-', ''))
        data = zipped_data.read(name)
    except KeyError:
        file_names = zipped_data.namelist()
        if len(file_names) == 1:
            name = file_names[0]
            if os.path.splitext(name)[1] == '.xml':
                data = zipped_data.read(name)
            else:
                raise KeyError('No XML file found in archive.')
        else:
            logger.info(
                'Found multiple files in archive. Only reading first XML file.')
            for name in file_names:
                if os.path.splitext(name)[1] == '.xml':
                    data = zipped_data.read(name)
                    break
            else:
                raise KeyError('No XML file found in archive.')
    return data


def save_to_disk(data, output_path):
    with open(output_path, 'w') as f:
        f.write(data)
