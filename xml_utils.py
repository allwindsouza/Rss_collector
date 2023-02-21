import xml.etree.ElementTree as ET
import requests
import re

def elements_equal(elem1, elem2):
    if elem1.tag != elem2.tag:
        return False
    if elem1.text != elem2.text:
        return False
    if len(elem1) != len(elem2):
        return False
    if not all(elements_equal(c1, c2) for c1, c2 in zip(elem1, elem2)):
        return False
    if "date" in elem1.attrib or "time" in elem1.attrib:
        return True  # Return True for date or time-related fields
    for key in elem1.attrib:
        if re.search(r"(date|time)", key):
            return True  # Return True for date or time-related fields
        if elem1.attrib[key] != elem2.attrib.get(key):
            return False
    return True


def compare_xml_files(data_1, data_2):
    """
    Compare two XML files, ignoring any differences in date or time-related fields.
    """
    # Load XML files into ElementTree objects
    tree1 = ET.ElementTree(ET.fromstring(data_1))
    tree2 = ET.ElementTree(ET.fromstring(data_2))


    # Get root elements of both trees
    root1 = tree1.getroot()
    root2 = tree2.getroot()

    # Check for differences in non-date and non-time-related fields
    for elem1, elem2 in zip(root1.iter(), root2.iter()):
        if not elements_equal(elem1, elem2):
            if not re.search(r"(date|time)", elem1.tag):
                for key in elem1.attrib:
                    if not re.search(r"(date|time)", key):
                        if elem1.attrib[key] != elem2.attrib.get(key):
                            return False
    return True

def download_and_write(url, path):
    response = requests.get(url)
    data = response.text
    with open(path, 'w') as file:
        file.write(data)
#
#
file_1 = "/home/allwind/Desktop/CAS/Rss_collector/resources/test_file_1.xml"
file_2 = "/home/allwind/Desktop/CAS/Rss_collector/resources/test_file_2.xml"

# download_and_write('http://3.220.4.21:3200/rss/f5055/1673600573.5898323.xml',
#                    '/home/allwind/Desktop/CAS/Rss_collector/resources/test_file_1.xml')
# download_and_write('http://3.220.4.21:3200/rss/f5055/1673600948.218719.xml',
#                    '/home/allwind/Desktop/CAS/Rss_collector/resources/test_file_2.xml')

# cond = compare_xml_files(file_1, file_2)
# print(cond)
