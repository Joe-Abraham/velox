# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# noinspection PyUnresolvedReferences
from docutils import nodes


# noinspection PyDefaultArgument,PyUnusedLocal
def issue_role(role, rawtext, text, lineno, inliner, options={}, content=[]):
    title = "#" + text
    link = "https://github.com/facebookincubator/velox/issues/" + text
    node = nodes.reference(text=title, refuri=link, **options)
    return [node], []


def setup(app):
    app.add_role("issue", issue_role)

    return {
        "parallel_read_safe": True,
    }
