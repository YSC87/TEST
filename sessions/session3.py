import operator
import functools
import copy


def tuple_types(input_tuple):
    """Return the tuple of types of elements in the given tuple.

    Parameters
    ----------
    input_tuple : tuple

    Returns
    -------
    out : tuple
    """
    return tuple(type(item) for item in input_tuple)


def remove_tuple_element(input_tuple, element):
    """Return the tuple without element(s).

    Parameters
    ----------
    input_tuple : tuple
    element : any

    Returns
    -------
    out : tuple
    """
    output = []
    for item in input_tuple:
        # The type is also checked to handle implicit conversion.
        # For example, 1 and True will both be excluded if the element is 1
        if type(item) != type(element) or item != element:
            output.append(item)
    return tuple(output)


def check_containment(input_string, lookup_string):
    """Check whether a substring can be found in the given string.

    Parameters
    ----------
    input_string : string
    lookup_string : string

    Returns
    -------
    out : bool
    """
    return lookup_string in input_string


def reverse(input_string):
    """Return the reversed version of the given string.

    Parameters
    ----------
    input_string : string

    Returns
    -------
    out : string
    """
    return input_string[::-1]


def concatenate(list1, list2):
    """Return the list concatenating two lists index-wise.
    The addition operator should exist for the elements of two inputted lists.
    The length of the resulting list is min(len(list1), len(list2)).

    Parameters
    ----------
    list1 : list
    list2 : list

    Returns
    -------
    out : list
    """
    return [element1 + element2 for element1, element2 in zip(list1, list2)]


def concatenate_list_of_lists(input_list):
    """Return the list concatenating elements of each sublist.
    The addition operator should exist for the elements of each sublist.
    The length of the resulting list is min(len(each sublist)).

    Parameters
    ----------
    input_list : list of lists

    Returns
    -------
    out : list
    """
    output = []
    for elements in zip(*input_list):
        output.append(functools.reduce(operator.add, elements))
    return output


def remove_list_element(input_list, element):
    """Return the list without element(s).

    Parameters
    ----------
    input_list : list
    element : any

    Returns
    -------
    out : list
    """
    output = []
    for item in input_list:
        # The type is also checked to handle implicit conversion.
        # For example, 1 and True will both be excluded if the element is 1
        if type(item) != type(element) or item != element:
            output.append(item)
    return tuple(output)


def deep_copy(input_list):
    """Return the deep copy of the given list

    Parameters
    ----------
    input_list : list

    Returns
    -------
    out : list
    """
    return copy.deepcopy(input_list)


def find(input_dict):
    """Return the tuple of all pairs of key and value.
    The deepest layers will be traversed if wrapped dicts are encountered.

    Parameters
    ----------
    input_dict : dict

    Returns
    -------
    out : tuple
    """
    output = tuple()
    for key, value in input_dict.items():
        output += find(value) if isinstance(value, dict) else ((key, value),)
    return output


def min_value(input_dict):
    """Return the key corresponding to the min value from the given dictionary.

    Parameters
    ----------
    input_dict : dict

    Returns
    -------
    out : string
    """
    return min(input_dict.items(), key=lambda pair: pair[1])[0]


if __name__ == "__main__":

    # Problem 1
    print("Problem 1")
    print("-" * 30)
    input_tuple = (1, 'a', 23, True)
    print(f"Input : {input_tuple}")
    print(f"Output : {tuple_types(input_tuple)}")
    print()

    # Problem 2
    print("Problem 2")
    print("-" * 30)
    input_tuple = (1, '1', 1, 23, True)
    print(f"Input : {input_tuple}")
    print(f"Output : {remove_tuple_element(input_tuple, 1)}")
    print()

    # Problem 3
    print("Problem 3")
    print("-" * 30)
    string, substr1, substr2 = 'abba', 'aba', 'bb'
    print(f"\"{substr1}\" is a substring of \"{string}\" ? {check_containment(string, substr1)}")
    print(f"\"{substr2}\" is a substring of \"{string}\" ? {check_containment(string, substr2)}")
    print()

    # Problem 4
    print("Problem 4")
    print("-" * 30)
    string = 'fall2022'
    print(f"Input : {string}")
    print(f"Output : {reverse(string)}")
    print()

    # Problem 5
    print("Problem 5")
    print("-" * 30)
    list1, list2 = [1, 2, 3, 4], [5, 6, 7, 8]
    print(f"Input : {list1} , {list2}")
    print(f"Output : {concatenate(list1, list2)}")
    print()
    list1, list2 = ['a', 'b', 'c', 'd'], ['z', 'y', 'x', 'w']
    print(f"Input : {list1} , {list2}")
    print(f"Output : {concatenate(list1, list2)}")
    print()

    # Problem 6
    print("Problem 6")
    print("-" * 30)
    input_list = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]
    print(f"Input : {input_list}")
    print(f"Output : {concatenate_list_of_lists(input_list)}")
    print()
    input_list = [['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']]
    print(f"Input : {input_list}")
    print(f"Output : {concatenate_list_of_lists(input_list)}")
    print()

    # Problem 7
    print("Problem 7")
    print("-" * 30)
    input_list = [0, '0', 99, 0, False]
    print(f"Input : {input_list}")
    print(f"Output : {remove_list_element(input_list, 0)}")
    print()

    # Problem 8
    print("Problem 8")
    print("-" * 30)
    list1 = [[1, 2, 3], [4, 5, 6]]
    list2 = deep_copy(list1)
    print(f"List 1 : {list1} ; The deep copy : {list2}")
    list1[0][1] = 9999
    print("After modifying the list1 ->")
    print(f"List 1 : {list1} ; The deep copy : {list2}")
    print()

    # Problem 9
    print("Problem 9")
    print("-" * 30)
    input_dict = {
        1: 111,
        3: 333,
        5: {
            4: 444,
            6: {
                7: 777,
                8: 888
            }
        },
        9: 999
    }
    print(f"Input : {input_dict}")
    print(f"Output : {find(input_dict)}")
    print()

    # Problem 10
    print("Problem 10")
    print("-" * 30)
    input_dict = {
        "NYC": 10,
        "BST": 20,
        "TYU": 5,
        "SEA": 99
    }
    print(f"Input : {input_dict}")
    print(f"Output : {min_value(input_dict)}")
    print()