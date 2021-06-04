# coding: utf-8

"""
  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  A copy of the License is located at

      http://www.apache.org/licenses/LICENSE-2.0

  or in the "license" file accompanying this file. This file is distributed
  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  express or implied. See the License for the specific language governing
  permissions and limitations under the License.
"""


"""
    ProductAdvertisingAPI

    https://webservices.amazon.com/paapi5/documentation/index.html  # noqa: E501
"""


import pprint
import re  # noqa: F401

import six

from paapi5_python_sdk.models.browse_node_ancestor import BrowseNodeAncestor  # noqa: F401,E501
from paapi5_python_sdk.models.browse_node_child import BrowseNodeChild  # noqa: F401,E501


class BrowseNode(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """

    """
    Attributes:
      swagger_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    swagger_types = {
        'ancestor': 'BrowseNodeAncestor',
        'children': 'list[BrowseNodeChild]',
        'context_free_name': 'str',
        'display_name': 'str',
        'id': 'str',
        'is_root': 'bool',
        'sales_rank': 'int'
    }

    attribute_map = {
        'ancestor': 'Ancestor',
        'children': 'Children',
        'context_free_name': 'ContextFreeName',
        'display_name': 'DisplayName',
        'id': 'Id',
        'is_root': 'IsRoot',
        'sales_rank': 'SalesRank'
    }

    def __init__(self, ancestor=None, children=None, context_free_name=None, display_name=None, id=None, is_root=None, sales_rank=None):  # noqa: E501
        """BrowseNode - a model defined in Swagger"""  # noqa: E501

        self._ancestor = None
        self._children = None
        self._context_free_name = None
        self._display_name = None
        self._id = None
        self._is_root = None
        self._sales_rank = None
        self.discriminator = None

        if ancestor is not None:
            self.ancestor = ancestor
        if children is not None:
            self.children = children
        if context_free_name is not None:
            self.context_free_name = context_free_name
        if display_name is not None:
            self.display_name = display_name
        if id is not None:
            self.id = id
        if is_root is not None:
            self.is_root = is_root
        if sales_rank is not None:
            self.sales_rank = sales_rank

    @property
    def ancestor(self):
        """Gets the ancestor of this BrowseNode.  # noqa: E501


        :return: The ancestor of this BrowseNode.  # noqa: E501
        :rtype: BrowseNodeAncestor
        """
        return self._ancestor

    @ancestor.setter
    def ancestor(self, ancestor):
        """Sets the ancestor of this BrowseNode.


        :param ancestor: The ancestor of this BrowseNode.  # noqa: E501
        :type: BrowseNodeAncestor
        """

        self._ancestor = ancestor

    @property
    def children(self):
        """Gets the children of this BrowseNode.  # noqa: E501


        :return: The children of this BrowseNode.  # noqa: E501
        :rtype: list[BrowseNodeChild]
        """
        return self._children

    @children.setter
    def children(self, children):
        """Sets the children of this BrowseNode.


        :param children: The children of this BrowseNode.  # noqa: E501
        :type: list[BrowseNodeChild]
        """

        self._children = children

    @property
    def context_free_name(self):
        """Gets the context_free_name of this BrowseNode.  # noqa: E501


        :return: The context_free_name of this BrowseNode.  # noqa: E501
        :rtype: str
        """
        return self._context_free_name

    @context_free_name.setter
    def context_free_name(self, context_free_name):
        """Sets the context_free_name of this BrowseNode.


        :param context_free_name: The context_free_name of this BrowseNode.  # noqa: E501
        :type: str
        """

        self._context_free_name = context_free_name

    @property
    def display_name(self):
        """Gets the display_name of this BrowseNode.  # noqa: E501


        :return: The display_name of this BrowseNode.  # noqa: E501
        :rtype: str
        """
        return self._display_name

    @display_name.setter
    def display_name(self, display_name):
        """Sets the display_name of this BrowseNode.


        :param display_name: The display_name of this BrowseNode.  # noqa: E501
        :type: str
        """

        self._display_name = display_name

    @property
    def id(self):
        """Gets the id of this BrowseNode.  # noqa: E501


        :return: The id of this BrowseNode.  # noqa: E501
        :rtype: str
        """
        return self._id

    @id.setter
    def id(self, id):
        """Sets the id of this BrowseNode.


        :param id: The id of this BrowseNode.  # noqa: E501
        :type: str
        """

        self._id = id

    @property
    def is_root(self):
        """Gets the is_root of this BrowseNode.  # noqa: E501


        :return: The is_root of this BrowseNode.  # noqa: E501
        :rtype: bool
        """
        return self._is_root

    @is_root.setter
    def is_root(self, is_root):
        """Sets the is_root of this BrowseNode.


        :param is_root: The is_root of this BrowseNode.  # noqa: E501
        :type: bool
        """

        self._is_root = is_root

    @property
    def sales_rank(self):
        """Gets the sales_rank of this BrowseNode.  # noqa: E501


        :return: The sales_rank of this BrowseNode.  # noqa: E501
        :rtype: int
        """
        return self._sales_rank

    @sales_rank.setter
    def sales_rank(self, sales_rank):
        """Sets the sales_rank of this BrowseNode.


        :param sales_rank: The sales_rank of this BrowseNode.  # noqa: E501
        :type: int
        """

        self._sales_rank = sales_rank

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(BrowseNode, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, BrowseNode):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
