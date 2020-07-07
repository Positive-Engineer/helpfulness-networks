#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

"""
__author__ = "SAI"
__license__ = "GPLv3"
__status__ = "Dev"
__version__ = '0.5'

import ipaddress
from random import shuffle
from itertools import zip_longest
from zlib import compress
from base64 import encodebytes
import pickle
from typing import (Any,
                    Iterable,
                    Iterator,
                    List,
                    Set,
                    )


def return_exclude_nets() -> Set[List[ipaddress.IPv4Network]]:
    """
    функция возвращает set из ip_network объектов,
    которые являются приватными или служебными сетями
    в пространстве Internet\n
    :return: set(List(ipaddress.IPv4Network))
    """
    ex = ipaddress.IPv4Address._constants  # видимо не правильно так делать.
    need_attribs = [name for name in dir(ex) if 'network' in name]
    _exclude_networks = []
    for n in need_attribs:
        _tmp = getattr(ex, n)
        if isinstance(_tmp, list):
            _exclude_networks.extend(_tmp)
        else:
            _exclude_networks.append(_tmp)
    dels: List[ipaddress.IPv4Network] = [ipaddress.ip_network('0.0.0.0/8'),
            ipaddress.ip_network('127.0.0.0/8'),
            ipaddress.ip_network('224.0.0.0/4'),
            ipaddress.ip_network('10.0.0.0/8'),
            ipaddress.ip_network('240.0.0.0/4'),
            ipaddress.ip_network('255.255.255.255/32'),
            ipaddress.ip_network('240.0.0.0/4')
            ]
    _exclude_networks.extend(dels)
    return set(_exclude_networks)



def grouper_generation(count: int,
                       iterable: Iterable,
                       fillvalue: Any = None) -> Iterator[list]:
    """
    :param count: length of subblock
    :param iterable: array of data
    :param fillvalue: is fill value in last chain
    :return:
    grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    генератор блоков по count элементов из списка iterable
    """
    args = [iter(iterable)] * count
    for element in zip_longest(fillvalue=fillvalue, *args):
        tmp = filter(lambda y: y is not None, element)
        yield list(tmp)


def load_to_memory_from_file(pathfile: str,
                             skip_error_netmask: bool = False) -> List:
    """

    :param pathfile:
    :param skip_error_netmask:
    :return:
    """
    with open(pathfile, 'r') as f:
        _nets = [row[:-1] for row in f]
    result = [[], []]
    for _net in _nets:
        net = None
        try:
            net = ipaddress.ip_network(_net, strict=True)
        except Exception as e:
            if skip_error_netmask:
                net = ipaddress.ip_network(_net, strict=False)
        if net:
            if net.num_addresses % 256 == 0 and net.num_addresses >= 256:
                result[0].extend([int(n.network_address) for n in net.subnets(new_prefix=24)])
            else:
                result[1].extend([int(n.network_address) for n in net.subnets(new_prefix=32)])
    return [list(set(result[0])), list(set(result[1]))]


def load_to_memory_from_list(list_networks: List[str],
                             skip_error_netmask: bool = False) -> List:
    """
    Функция на вход получает список из строк - каждая строка представляет собой запись сети\n
    skip_error_netmask - проверка на ошибочность записи\n
    результат функции - list из 2 List\n
    в list0 - целые числа - представления подсети /24 - ее адреса\n
    в list1 - целые числа - представления ip адреса, или же(то же самое) - подсети /32\n
    :param list_networks:
    :param skip_error_netmask:
    :return:
    """
    result: List[list, list] = [[], []]
    if not list_networks:
        return result
    for _net in list_networks:
        net = None
        try:
            net = ipaddress.ip_network(_net, strict=True)
        except Exception as e:
            if skip_error_netmask:
                net = ipaddress.ip_network(_net, strict=False)
        if net:
            if net.num_addresses >= 256:
                result[0].extend([int(n.network_address) for n in net.subnets(new_prefix=24)])
            else:
                result[1].extend([int(n.network_address) for n in net.subnets(new_prefix=32)])
    return [list(set(result[0])), list(set(result[1]))]


def return_clean_list(white_list: List,
                      black_list: List) -> List:
    """
    задачи фукнции - вернуть отфильтрованный список
    :param white_list:
    :param black_list:
    :return:
    """
    if not black_list:
        black_list = [[], []]
    nets_24: List[int] = list(set(white_list[0]) - set(black_list[0]))  # уже отфильтрованный список с целыми - сети 24
    nets_32: List[int] = white_list[1]  # переменная для хранения "новых" целых - которые являются сетями /32
    _temp_nets32_as_24 = {}  # словарь, для хранения - ключ - целое /24 сеть, значение список из сетей целых /32
    # итерируемся по списку целых /32
    for i in black_list[1]:
        # определяем к какой подсети(/24) относится сеть /32 из черного списка
        net24_int = int(ipaddress.ip_network(i).supernet(new_prefix=24).network_address)
        # если этого целого нет в словаре, добавляем ключ и добавляем в лист значение самой /32
        # или просто добавляем в лист по соответствующему ключу в словаре
        if net24_int not in _temp_nets32_as_24:
            _temp_nets32_as_24[net24_int] = []
            _temp_nets32_as_24[net24_int].append(i)
        else:
            _temp_nets32_as_24[net24_int].append(i)
    # в завершении итераций по черному списку из /32 получается словарь
    # где ключ - это целое число - оно же подсеть /24
    # значения - это список из целых чисел - они же, простые ip адреса, и каждый /32 подсеть

    # итерируемся по ключам из получившейся структуры
    for k, v in _temp_nets32_as_24.items():
        if k in nets_24:
            # если ключ k есть в уже отфильтрованном списке с белыми целыми - списке сетей 24
            # то тогда из списка подсетей /24 его необходимо удалить
            nets_24.pop(nets_24.index(k))
            # в _tmp_ips запишем все ip адреса(они же сети /32) из только что "удаленной"(этой) сети /24
            _tmp_ips = [int(ip) for ip in ipaddress.ip_network(k).supernet(new_prefix=24)]
            # вычитаем из множеста всех  ip адресов этой /24 подсети ip адреса из черного списка
            # все адреса в целых
            only_need_32 = list(set(_tmp_ips) - set(v))
            # расширем общий список белых остатком от вычитания, чтоб его не потерять при скане
            nets_32.extend(only_need_32)
    # повторно вычтем из общего списка сетей /32 - все сети /32 из черного списка, видимо лишнее действие?
    nets_32 = list(set(nets_32) - set(black_list[1]))
    # итого возвращаем список из двух списков
    # в первом списке - целые чила - которые представляют собой все подсети /24
    # во втором списке - целые числа - которые представляют собой все подсети /32
    # print(len(nets_24), len(nets_32))
    # region удалить 0 и последний
    nets_32 = [net32 for net32 in nets_32 if (net32 % 256 != 0)]  # 192.168.1.0
    nets_32 = [net32 for net32 in nets_32 if (net32 % 256 != 255)]  # 192.168.1.255
    return [nets_24, nets_32]


def create_list_for_task_from_lists(whitelist: List[str],
                                    blacklist: List[str],
                                    skip_error_netmask: bool = False) -> List:
    """

    :param whitelist: список с CIDR записями сетей (список целей)
    :param blacklist: список с CIDR записями сетей (список исключений)
    :param skip_error_netmask: флаг того, что не проверять на валидность записи CIDR сети
    :return: список(лист) как результат функции return_clean_list - список из 2 списков
    """
    white_list: List = load_to_memory_from_list(whitelist, skip_error_netmask)
    black_list: List = load_to_memory_from_list(blacklist, skip_error_netmask)
    result: List = return_clean_list(white_list, black_list)
    return result


def create_list_for_task_from_file(whitelist_path: str,
                                   blacklist_path: str,
                                   skip_error_netmask: bool = False) -> List:
    """
    на вход функции - пути к файлам, на выходе - возвращает список сетей  в целочисленном представлении(без перемешивания)\n
    :param whitelist_path:  путь к файлу с CIDR записями сетей (список целей)
    :param blacklist_path: путь к файлу с CIDR записями сетей (список исключений)
    :param skip_error_netmask: флаг того, что не проверять на валидность записи CIDR сети
    :return: список(лист) как результат функции return_clean_list - список из 2 списков
    """
    white_list: List = load_to_memory_from_file(whitelist_path, skip_error_netmask)
    black_list: List = load_to_memory_from_file(blacklist_path, skip_error_netmask)
    result: List = return_clean_list(white_list, black_list)
    return result


def create_list_for_task_shuffled_from_file(whitelist_path: str,
                                            blacklist_path: str,
                                            skip_error_netmask: bool = False) -> List:
    """
    на вход функции - пути к файлам, на выходе - возвращает список сетей  в целочисленном представлении(c перемешиванием)\n
       :param whitelist_path:  путь к файлу с CIDR записями сетей (список целей)
    :param blacklist_path: путь к файлу с CIDR записями сетей (список исключений)
    :param skip_error_netmask: флаг того, что не проверять на валидность записи CIDR сети
    :return: список(лист) как результат функции return_clean_list - список из 2 списков
    """
    white_list: List  = load_to_memory_from_file(whitelist_path, skip_error_netmask)
    black_list: List  = load_to_memory_from_file(blacklist_path, skip_error_netmask)
    _nets: List = return_clean_list(white_list, black_list)
    n24: List[int] = _nets[0]
    n32: List[int] = _nets[1]
    shuffle(n24)
    shuffle(n32)
    return [n24, n32]


def create_list_for_task_shuffled_from_list(whitelist: List[str],
                                            blacklist: List[str],
                                            skip_error_netmask: bool = False) -> List:
    white_list: List = load_to_memory_from_list(whitelist, skip_error_netmask)
    black_list: List  = load_to_memory_from_list(blacklist, skip_error_netmask)
    _nets: List = return_clean_list(white_list, black_list)
    n24: List[int] = _nets[0]
    n32: List[int] = _nets[1]
    shuffle(n24)
    shuffle(n32)
    return [n24, n32]


def create_blocks_for_tasks(count24: int,
                            count32: int,
                            one_big_block: List) -> Iterator:
    """

    :param count24: целое - сколько подсетей /24 в 1 блоке
    :param count32: целое - сколько подсетей /32 в 1 блоке
    :param one_big_block: - тот самый список из 2 список с сетями 23 и 32
    :return: список, где каждый элемент состоит из двух списков с сетями 24 и сетями 32 - в количестве как на вход функции \n
    """
    n24: List[int] = one_big_block[0]
    n32: List[int] = one_big_block[1]
    n24_blocks = list(grouper_generation(count24, n24))
    n32_blocks = list(grouper_generation(count32, n32))
    result = zip_longest(n24_blocks, n32_blocks)
    return result


def create_blocks_for_tasks_shuffled(count24: int,
                                     count32: int,
                                     one_big_block: List) -> Iterator:
    """
    :param count24: целое - сколько подсетей /24 в 1 блоке
    :param count32: целое - сколько подсетей /32 в 1 блоке
    :param one_big_block: - тот самый список из 2 список с сетями 23 и 32
    :return: список, где каждый элемент состоит из двух списков с сетями 24 и сетями 32 - в количестве как на вход функции \n
    """
    n24: List[int] = one_big_block[0]
    n32: List[int] = one_big_block[1]
    shuffle(n24)
    shuffle(n32)
    n24_blocks = list(grouper_generation(count24, n24))
    n32_blocks = list(grouper_generation(count32, n32))
    result = zip_longest(n24_blocks, n32_blocks)
    return result


def create_blocks_ip_tasks_shuffled_from_file(pathtofile: str,
                                              count: int) -> List[List]:
    """

    :param pathtofile:
    :param count:
    :return:
    """
    list_ip_int: List[int] = []
    with open(pathtofile, 'r') as source:
        rows = [row[:-1] for row in source]
        for row in rows:
            try:
                ip_int = int(ipaddress.ip_address(row))
                list_ip_int.append(ip_int)
            except Exception as e:
                print(e)
    shuffle(list_ip_int)
    result = list(grouper_generation(count, list_ip_int))
    return result


def create_blocks_ip_tasks_shuffled_from_files(pathtofiles: List[str],
                                               count: int) -> List[List]:
    """

    :param pathtofiles:
    :param count:
    :return:
    """
    list_ip_int: List[int] = []
    for pathtofile in pathtofiles:
        with open(pathtofile, 'r') as source:
            rows = [row[:-1] for row in source]
            for row in rows:
                try:
                    ip_int = int(ipaddress.ip_address(row))
                    list_ip_int.append(ip_int)
                except Exception as e:
                    print(e)
    list_ip_int = list(set(list_ip_int))
    shuffle(list_ip_int)
    result = list(grouper_generation(count, list_ip_int))
    return result


def pickle_block(a: List) -> str:
    """
    сжатие и представление списка в base64(список целых чисел в bytes и потом в base64)
    такое представление данных необходимо для "отгрузки" в json и позже в очередь SQS\n
    для сжатия используется zlib с level=9
    :param a:
    :return:
    """
    list_ints_bytes = compress(pickle.dumps(a), level=9)
    result = encodebytes(list_ints_bytes).decode('utf-8')
    return result


def save_real_internet_ipv4_pickle(path_to_save: str) -> bool:
    """
    вспомогательная функция, редко используется, необходима для формирования pickle файла с адресами Интернет
    :param path_to_save:
    :return:
    """
    try:
        black_list = [str(n) for n in return_exclude_nets()]
        all_internet = ["0.0.0.0/0"]
        clean_internet = create_list_for_task_from_lists(all_internet, black_list, False)
        with open(path_to_save, 'wb') as inet_pickle:
            pickle.dump(clean_internet, inet_pickle)
        return True
    except:
        return False


def save_targets_networks_to_file(paht_to_outfile: str,
                                  white_list: List[str],
                                  black_list: List[str] = None,
                                  sorted_nets: bool = True,
                                  save_net32_as_ip: bool = False):
    """
    функция сохранения записей целей в файл\n

    :param paht_to_outfile: файл(путь) в который в текстовом виде будут сохранены данные
    :param white_list: list с записями в str адресов подсетей(['192.168.0.0/16', '192.200.1.0/24'...])
    :param black_list: blacklist с записями в str адресов подсетей(['192.168.0.0/16', '192.200.1.0/24'...])
    :param sorted_nets: флаг, сортировать результирующий список или перемешать его - True - сортировать
    :param save_net32_as_ip: флаг, сохранять ipaddress 192.168.1.1/32 в CIDR или просто 192.168.1.1
    :return:
    """
    _white_list = load_to_memory_from_list(white_list)
    _black_list = load_to_memory_from_list(black_list)
    _nets = return_clean_list(_white_list, _black_list)
    networks = []
    with open(paht_to_outfile, 'w', encoding='utf-8') as outfile:
        n24, n32 = _nets
        if sorted_nets:
            n24.sort()
            n32.sort()
        else:
            shuffle(n24)
            shuffle(n32)
        if n24:
            for net in filter(lambda z: z, n24):
                networks.append(str(ipaddress.ip_network(net).supernet(new_prefix=24)))
        if n32:
            for net in filter(lambda z: z, n32):
                if save_net32_as_ip:
                    networks.append(str(ipaddress.ip_address(net)))
                else:
                    networks.append(str(ipaddress.ip_network(net)))
        outfile.write('\n'.join(networks)+'\n')


def create_subblocks_for_tasks_from_lists(count_net24: int,
                               count_net32: int,
                               white_list: List[str],
                               black_list: List[str] = None) -> Iterator[List]:
    _white_list = load_to_memory_from_list(white_list)
    _black_list = load_to_memory_from_list(black_list)
    _nets = return_clean_list(_white_list, _black_list)
    result_blocks = create_blocks_for_tasks_shuffled(count_net24, count_net32, _nets)
    return result_blocks


def create_subblocks_for_tasks(count_net24: int,
                               count_net32: int,
                               path_to_inet: str,
                               black_list: List[str] = None) -> Iterator[List]:
    with open(path_to_inet, 'rb') as inetfile:
        _white_list = pickle.load(inetfile)
    _black_list = load_to_memory_from_list(black_list)
    _nets = return_clean_list(_white_list, _black_list)
    result_blocks = create_blocks_for_tasks_shuffled(count_net24, count_net32, _nets)
    return result_blocks
