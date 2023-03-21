import xml.etree.ElementTree as ET
import requests
import re

def download_and_write(url, path):
    response = requests.get(url)
    data = response.text
    with open(path, 'w') as file:
        file.write(data)


def elements_equal(elem1, elem2):
    try:
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
    except Exception as e:
        print(f" exception enc : {e}")
        return False


def compare_xml_files(data_1, data_2):
    """
    Compare two XML files, ignoring any differences in date or time-related fields.
    """
    try:
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
    except Exception as e:
        print(f" exception enc : {e}")
        return False
    return True


def read_file(path):
    with open(path, 'r') as file:
        data = file.read()
    return data



url_1 = "http://3.220.4.21:3200/rss/fe602/1674201285.3219645.xml"
url_2 = "http://3.220.4.21:3200/rss/fe602/1674201998.061975.xml"

file_1 = "/home/allwind/Desktop/CAS/Rss_collector/test_docs/01.xml"
file_2 = "/home/allwind/Desktop/CAS/Rss_collector/test_docs/02.xml"

# download_and_write("http://3.220.4.21:3200/rss/48735/1674200577.920884.xml", file_1)
# download_and_write("http://3.220.4.21:3200/rss/48735/1674201335.6093478.xml", file_2)

data_1 = read_file(file_1)
data_2 = read_file(file_2)

print(compare_xml_files(data_1, data_2))
