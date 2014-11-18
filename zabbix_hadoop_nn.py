__author__ = 'ahmed'

import json
import ast
import textwrap
import urllib
import logging

import time
import datetime

from xml.dom import minidom
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement

import argparse
import re

import zbxsend  # This package can be installed from : https://github.com/zubayr/zbxsend


def temp_json_loading():
    file_desc = open('namenode_jmx.json', 'r+')
    data = json.load(file_desc)
    return data

def generate_module_dictionary(category_to_process, data):
    module_name = dict()
    for item in category_to_process:
        for key in data['beans'][item]:
            if key == 'name':
                try:
                    if 'type' in str(data['beans'][item][key]):
                        module_name[item] = str(data['beans'][item][key]).split('=')[1]
                    elif 'name' in str(data['beans'][item][key]):
                        module_name[item] = str(data['beans'][item][key]).split('name=')[1]
                except:
                    print("Some Error Occured in module_gen - But will continue for other modules")
                    continue

    return module_name

# This function converts the servername to URL which we need to query.
def get_url(server_name, listen_port):

    if listen_port < 0:
        print ("Invalid Port")
        exit()

    if not server_name:
        print("Pass valid Hostname")
        exit()

    URL = "http://"+server_name+":"+str(listen_port)+"/jmx"
    return URL


def load_url_as_dictionary(url):
    # Server URL to get JSON information
    return json.load(urllib.urlopen(url))

def check_value_type(value):
    if isinstance(value, int):
        return int(value)
    elif isinstance(value, float):
        return float(value)
    else:
        return str(value).strip()

def processing_json(category, json_data, module_name):
    # Comments
    send_value = dict()
    for item in category:
        for key in json_data['beans'][item]:

            if key == 'name':
                continue

            elif not isinstance(json_data['beans'][item][key], dict) and \
                    not isinstance(json_data['beans'][item][key], list):
                zbx_key = re.sub('[\[\]/=*:\.,\'\"><]', '', str(module_name[item] + '_' + key).strip())
                zbx_value = json_data['beans'][item][key]
                send_value[zbx_key] = zbx_value

            elif isinstance(json_data['beans'][item][key], dict):
                for value_in_sub in json_data['beans'][item][key]:
                    zbx_key = re.sub('[\[\]/=*:\.,\'\"><]', '', str(module_name[item] + '_' + key + '_' + value_in_sub).strip())
                    zbx_value = json_data['beans'][item][key][value_in_sub]
                    send_value[zbx_key] = zbx_value

            if key == "LiveNodes":
                dict_v = ast.literal_eval(json_data['beans'][item][key])
                for key_live in dict_v:
                    for item_live in dict_v[key_live]:
                        zbx_key = re.sub('[\[\]/=*:\.,\'\"><]', '', str(module_name[item] + '_' + key + '_' + key_live + '_' + item_live).strip())
                        zbx_value = dict_v[key_live][item_live]
                        send_value[zbx_key] = zbx_value
    return send_value


def send_data_to_zabbix(send_data_from_dict, host_name, zbx_server_ip, zbx_server_port):
    clock = time.time()
    send_data_list = []
    for keys in send_data_from_dict:
        send_data_list.append(zbxsend.Metric(host_name, keys, send_data_from_dict[keys], clock))

    zbxsend.send_to_zabbix(send_data_list, zabbix_host=zbx_server_ip, zabbix_port=zbx_server_port)



# --------------------------------------------------------
# Generate Complete Export/Import XML File
# --------------------------------------------------------
def generate_items_xml_file_complete(
                                    list_from_file,
                                    host_name,
                                    host_group_name,
                                    host_interface,
                                    item_application_name=None):

    # Date format for the new file created.
    fmt = '%Y-%m-%dT%H:%M:%SZ'

    # Creating the main element.
    zabbix_export = Element('zabbix_export')

    # Sub Element which fall under zabbix_export
    version = SubElement(zabbix_export, 'version')
    date =  SubElement(zabbix_export, 'date')

    # Groups
    groups = SubElement(zabbix_export, 'groups')
    group_under_groups = SubElement(groups, 'group')
    name_under_group = SubElement(group_under_groups, 'name')

    # triggers
    SubElement(zabbix_export, 'triggers')

    # hosts
    hosts = SubElement(zabbix_export, 'hosts')
    host_under_hosts = SubElement(hosts, 'host')
    host_under_host = SubElement(host_under_hosts, 'host')
    name_under_host = SubElement(host_under_hosts, 'name')

    SubElement(host_under_hosts, 'proxy')

    # status and its sub elements
    status_under_host = SubElement(host_under_hosts, 'status')
    ipmi_authtype_under_host = SubElement(host_under_hosts, 'ipmi_authtype')
    ipmi_privilege_under_host = SubElement(host_under_hosts, 'ipmi_privilege')

    # elements under hosts
    SubElement(host_under_hosts, 'ipmi_username')
    SubElement(host_under_hosts, 'ipmi_password')
    SubElement(host_under_hosts, 'templates')

    # Groups under a hosts
    groups_under_hosts = SubElement(host_under_hosts, 'groups')
    group_under_groups_host = SubElement(groups_under_hosts, 'group')
    name_group_under_groups_host = SubElement(group_under_groups_host, 'name')

    # Interfaces
    interfaces_under_host = SubElement(host_under_hosts, 'interfaces')
    interface_under_interfaces_host = SubElement(interfaces_under_host, 'interface')
    default_under_interface = SubElement(interface_under_interfaces_host, 'default')
    type_under_interface = SubElement(interface_under_interfaces_host, 'type')
    useip_under_interface = SubElement(interface_under_interfaces_host, 'useip')
    ip_under_interface = SubElement(interface_under_interfaces_host, 'ip')
    SubElement(interface_under_interfaces_host, 'dns')
    port_under_interface = SubElement(interface_under_interfaces_host, 'port')
    interface_ref_under_interface = SubElement(interface_under_interfaces_host, 'interface_ref')

    # elements under hosts
    applications = SubElement(host_under_hosts, 'applications')
    application = SubElement(applications, 'application')
    application_name = SubElement(application, 'name')
    items = SubElement(host_under_hosts, 'items')
    SubElement(host_under_hosts, 'discovery_rules')

    # macro sub element
    macros = SubElement(host_under_hosts, 'macros')
    macro = SubElement(macros, 'macro')
    sub_macro = SubElement(macro, 'macro')
    value = SubElement(macro, 'value')
    SubElement(host_under_hosts, 'inventory')

    # This information will be from the user.
    date.text = datetime.datetime.now().strftime(fmt)
    host_under_host.text = host_name
    name_under_host.text = host_name
    name_under_group.text = host_group_name
    ip_under_interface.text = host_interface
    name_group_under_groups_host.text = host_group_name

    # Standard values
    version.text = '2.0'
    status_under_host.text = '0'
    ipmi_authtype_under_host.text = '-1'
    ipmi_privilege_under_host.text = '2'
    default_under_interface.text = '1'
    type_under_interface.text = '1'
    useip_under_interface.text = '1'
    port_under_interface.text = '10050'
    interface_ref_under_interface.text = 'if1'
    sub_macro.text = '{$SNMP_COMMUNITY}'
    value.text = 'public'
    application_name.text = item_application_name

    #
    # Processing through the list of OID from the list in the dictionary
    # This actually a range as in the csv file
    #   If we have set 'all_oid_range' as true, then we will process all the OID range for each OID
    #   Warning : There will be too many Items in the import file.
    #             BE CAREFUL WITH THE RANGE.
    #
    for row_dict_from_file in list_from_file:
        item_creator(row_dict_from_file, items, row_dict_from_file, item_application_name, list_from_file[row_dict_from_file])


    return ElementTree.tostring(zabbix_export)


def item_creator(dictionary, items, module_detail_dict_item_from_dictionary, item_application_name, value_data):
    #
    # Creating an initial XML Template
    #
    item = SubElement(items, 'item')
    name = SubElement(item, 'name')
    type = SubElement(item, 'type')
    SubElement(item, 'snmp_community')
    multiplier = SubElement(item, 'multiplier')
    SubElement(item, 'snmp_oid')
    key = SubElement(item, 'key')
    delay = SubElement(item, 'delay')
    history = SubElement(item, 'history')
    trends = SubElement(item, 'trends')
    status = SubElement(item, 'status')
    value_type = SubElement(item, 'value_type')
    SubElement(item, 'allowed_hosts')                                   # If we are not using an element
                                                                        # then do not assign it
    SubElement(item, 'units')                                           #
    delta = SubElement(item, 'delta')
    SubElement(item, 'snmpv3_contextname')
    SubElement(item, 'snmpv3_securityname')
    snmpv3_securitylevel = SubElement(item, 'snmpv3_securitylevel')
    snmpv3_authprotocol = SubElement(item, 'snmpv3_authprotocol')
    SubElement(item, 'snmpv3_authpassphrase')
    snmpv3_privprotocol = SubElement(item, 'snmpv3_privprotocol')
    SubElement(item, 'snmpv3_privpassphrase')
    formula = SubElement(item, 'formula')
    SubElement(item, 'delay_flex')
    SubElement(item, 'params')
    SubElement(item, 'ipmi_sensor')
    data_type = SubElement(item, 'data_type')
    authtype = SubElement(item, 'authtype')
    SubElement(item, 'username')
    SubElement(item, 'password')
    SubElement(item, 'publickey')
    SubElement(item, 'privatekey')
    SubElement(item, 'port')
    description = SubElement(item, 'description')
    inventory_link = SubElement(item, 'inventory_link')
    SubElement(item, 'valuemap')
    applications = SubElement(item, 'applications')
    application = SubElement(applications, 'application')
    application_name = SubElement(application, 'name')
    interface_ref = SubElement(item, 'interface_ref')

    #
    # Setting basic information for the item. Setting Values now.
    #
    name.text = module_detail_dict_item_from_dictionary

    # This has to be unique
    key.text = module_detail_dict_item_from_dictionary

    # For Verbose Mode
    logging.debug('Key Generated as : ' + str(key.text))

    #
    # Setting value type to get information in int to string.
    # Based on the input file.
    # TODO : Add more datatype based on the return information.
    #
    if isinstance(value_data, str):
        value_type.text = '4'
    elif isinstance(value_data, int):
        value_type.text = '2'
    elif isinstance(value_data, float):
        value_type.text = '0'
    else:
        value_type.text = '4'


    #
    # Setting SNMP v1, This will change as per requirement.
    # TODO : Put a condition here so that we can change this on the fly.
    #
    type.text = '2'

    #
    # Creating Item with default values. No change here.
    # TODO : Need to add more information here based on requirement.
    #
    delta.text = '0'
    snmpv3_securitylevel.text = '0'
    snmpv3_authprotocol.text = '0'
    snmpv3_privprotocol.text = '0'
    formula.text = '1'
    data_type.text = '0'
    authtype.text = '0'
    inventory_link.text = '0'
    interface_ref.text = 'if1'
    delay.text = '30'
    history.text = '90'
    trends.text = '365'
    status.text = '0'
    multiplier.text = '0'

    # Adding Description as in the CSV file.
    description.text = 'Description For : ' + str(module_detail_dict_item_from_dictionary) + 'goes here.'

    # Creating all the items in a specific Application on Zabbix
    application_name.text = str(module_detail_dict_item_from_dictionary).split('_')[0]



def xml_pretty_me(file_name_for_prettify, string_to_prettify):
    #
    # Open a file and write to it and we are done.
    #
    logging.debug("Creating File pretty_%s", file_name_for_prettify)

    # Creating an XML and prettify xml.
    xml = minidom.parseString(string_to_prettify)
    pretty_xml_as_string = xml.toprettyxml()

    # Creating a file to write this information.
    output_file = open(file_name_for_prettify, 'w' )
    output_file.write(pretty_xml_as_string)

    # Done.
    logging.debug("Creation Complete")
    output_file.close()


def get_json_data_as_kv(hp_host_name, hp_host_port):
    #
    category_to_process = [0, 1, 4, 8, 14, 15, 16, 21, 23, 26, 27, 29]
    url_to_query = get_url(hp_host_name, hp_host_port)
    logging.debug('URL to Query : ' + url_to_query)

    json_data = load_url_as_dictionary(url_to_query)

    #json_data = temp_json_loading()
    create_modules = generate_module_dictionary(category_to_process, json_data)
    ready_dictionary = processing_json(category_to_process, json_data, create_modules)

    return ready_dictionary

if __name__ == "__main__":


    # create the top-level parser
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=textwrap.dedent('''

    Namenode Zabbix Monitoring
    ----------------------

    This script can be used to monitor Namenode Parameters.
    This script can be used to

    1. Generate Zabbix Import XML.
    2. Send monitoring data to Zabbix server.

    Parameter which are monitored are in the indexes of the JSON and are as below.
    category_to_process = [0, 1, 4, 8, 14, 15, 16, 21, 23, 26, 27, 29]

    ----------------------'''))

    parser.add_argument('-hh', '--hadoop-host-name', help='Hadoop Hostname/IP to connect to get JSON file.', required=True)
    parser.add_argument('-hp', '--hadoop-host-port', help='Hadoop Hostname/IP Port to connect to.', required=True)
    parser.add_argument('-zh', '--zabbix-host-name', help='Hostname as in the Zabbix server.', required=True)

    subparsers = parser.add_subparsers(help='sub-command help')

    # create the parser for the "xml-generator" command
    parser_a = subparsers.add_parser('xml-gen', help='\'xml-gen --help\' for more options')
    parser_a.add_argument('-zp', '--zabbix-host-port', help='Host port as as in the Zabbix server. (Monitoring host)', required=True)
    parser_a.add_argument('-zi', '--zabbix-host-interface', help='Host Interface as as in the Zabbix server.. (Monitoring host)', required=True)
    parser_a.add_argument('-zg', '--zabbix-host-group', help='Host Group as in the Zabbix server. (Monitoring host)', required=True)
    parser_a.add_argument('-za', '--zabbix-host-application', help='Host Application as in the Zabbix server. (Monitoring host)', required=True)


    # create the parser for the "send-data" command
    parser_b = subparsers.add_parser('send-data', help='\'send-data --help\' for more options')
    parser_b.add_argument('-zp', '--zabbix-port', default=10051, help='Zabbix port for sending data, default=10051')
    parser_b.add_argument('-zi', '--zabbix-server-ip', help='Zabbix server IP to send the Data to.', required=True)

    str_cmd = '-hh hmhdmaster1 -hp 50070 -zh hmhdmaster1 send-data -zp 10051 -zi 10.231.67.201'.split()
    str_cmd2 = '-hh hmhdmaster1 -hp 50070 -zh hmhdmaster1 xml-gen -zp 10050 -zi 10.20.6.31 -zg Linux_Server -za hadoop'.split()
    args = parser.parse_args()

    type_proc = ''
    try:
        if args.zabbix_server_ip:
            type_proc = 'SEND'
    except:
        if args.zabbix_host_port:
            type_proc = 'XML'


    if type_proc == 'SEND':
        hadoop_host_name = args.hadoop_host_name
        hadoop_host_port = args.hadoop_host_port
        zabbix_host_name = args.zabbix_host_name
        zabbix_port = args.zabbix_port
        zabbix_server_ip = args.zabbix_server_ip

        key_value_pairs = get_json_data_as_kv(hadoop_host_name, hadoop_host_port)
        #send_data_from_dict, host_name, zbx_server_ip, zbx_server_port
        send_data_to_zabbix(key_value_pairs, str(zabbix_host_name), str(zabbix_server_ip), int(zabbix_port))

    elif type_proc == 'XML':
        hadoop_host_name = args.hadoop_host_name
        hadoop_host_port = args.hadoop_host_port
        zabbix_host_name = args.zabbix_host_name
        zabbix_host_port = args.zabbix_host_port
        zabbix_host_interface = args.zabbix_host_interface
        zabbix_host_group = args.zabbix_host_group
        zabbix_host_application = args.zabbix_host_application

        key_value_pairs = get_json_data_as_kv(hadoop_host_name, hadoop_host_port)
        xml_string = generate_items_xml_file_complete(key_value_pairs, zabbix_host_name,
                                                      zabbix_host_group, zabbix_host_interface,
                                                      zabbix_host_application)
        xml_pretty_me(str(zabbix_host_name).lower() + '_' + zabbix_host_interface + '_export.xml', xml_string)
    else:
        parser.print_help()
