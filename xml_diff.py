import xml.etree.ElementTree as ET
import requests


def compare_xml_files(file1, file2):
    # Read the first XML file
    tree1 = ET.parse(file1)
    root1 = tree1.getroot()

    print("Root1.attrib: ", len(root1))

    return
    # Read the second XML file
    tree2 = ET.parse(file2)
    root2 = tree2.getroot()

    print("Root2.attrib: ", len(root2))

    # Define a function to recursively compare elements
    def compare_elements(elem1, elem2):
        # Compare the tag names of the elements
        if elem1.tag != elem2.tag:
            return False

        # Compare the attributes of the elements
        if elem1.attrib != elem2.attrib:
            return False

        # Compare the text content of the elements
        if elem1.text != elem2.text:
            return False

        # Compare the child elements of the elements
        if len(elem1) != len(elem2):
            return False
        for child1, child2 in zip(elem1, elem2):
            if not compare_elements(child1, child2):
                return False

        return True

    # Compare the roots of the XML files
    differences = []
    if not compare_elements(root1, root2):
        # Output the differences
        for elem1, elem2 in zip(root1, root2):
            if not compare_elements(elem1, elem2):
                differences.append((elem1.tag, ET.tostring(elem1), ET.tostring(elem2)))

    return differences


def download_and_write(url, path):
    response = requests.get(url)
    data = response.text
    with open(path, 'w') as file:
        file.write(data)


url_1 = "http://3.220.4.21:3200/rss/fe602/1674201285.3219645.xml"
url_2 = "http://3.220.4.21:3200/rss/fe602/1674201998.061975.xml"

file_1 = "/home/allwind/Desktop/CAS/Rss_collector/test_docs/03.xml"
file_2 = "/home/allwind/Desktop/CAS/Rss_collector/test_docs/04.xml"

# download_and_write("http://3.220.4.21:3200/rss/48735/1674200577.920884.xml", file_1)
# download_and_write("http://3.220.4.21:3200/rss/48735/1674201335.6093478.xml", file_2)


print(compare_xml_files(file_1, file_2))
