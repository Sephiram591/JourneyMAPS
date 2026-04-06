# MIT License
# -----------
#
# Copyright (c) 2019-2020 Simphony Project Contributors and others (see AUTHORS.txt)
#
# The resources, libraries, and some source files under other terms (see NOTICE.txt).
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""
jmaps
=====================

Description
"""

import platform
import sys

__version__ = "0.1.0"
__license__ = "MIT"
__project_url__ = "https://github.com/Sephiram591/JourneyMAPS"
__forum_url__ = "https://github.com/Sephiram591/JourneyMAPS"
__trouble_url__ = __project_url__ + "/wiki/Troubleshooting-Guide"
__website_url__ = "https://camacholab.byu.edu/"

# Public facade: expose the Journey API at the package root
from .config import PATH
from .journey import (
    Journey,
    get_filename,
    JBatch,
    JPath,
    PathResult,
    ExecutionType,
    PathOptions,
    REF_SEP,
    ResetCondition,
    JParam,
    JValue,
    InvisibleParam,
    JDict,
    Buffer,
    YBuffer,
    XBuffer,
    Refer,
    wrap_jparam,
    evaluate_keys,
    Base,
    get_sql_type,
    cast_sql_type,
    get_sql_schema,
    create_tables,
    DBPath,
    DBPathVersion,
    DBResult,
    # register,
    # serialize_default,
    # deserialize,
    # serializable,
    # deserializable,
)
from .paths import GDS_Tidy3DPath, GDS_TidyModePath

__all__ = [
    "PATH",
    "Journey",
    "PathOptions",
    "get_filename",
    "JBatch",
    "JPath",
    "PathResult",
    "ExecutionType",
    "REF_SEP",
    "ResetCondition",
    "JParam",
    "JValue",
    "InvisibleParam",
    "JDict",
    "Buffer",
    "YBuffer",
    "XBuffer",
    "Refer",
    "wrap_jparam",
    "Base",
    "get_sql_type",
    "cast_sql_type",
    "get_sql_schema",
    "create_tables",
    "DBPath",
    "DBPathVersion",
    "DBResult",
    "register",
    "write",
    "read",
    "writable",
    "readable",
    "evaluate_keys",
    "GDS_Tidy3DPath",
    "GDS_TidyModePath",
]