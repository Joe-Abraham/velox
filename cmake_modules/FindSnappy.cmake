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

# Find the Snappy compression library.
#
# This module defines the following variables:
#   SNAPPY_FOUND - True if Snappy library is found
#   SNAPPY_LIBRARY - The Snappy library
#   SNAPPY_INCLUDE_DIR - Path to Snappy include directory
#
# It also defines the following imported target:
#   Snappy::snappy - The Snappy library target

find_library(SNAPPY_LIBRARY NAMES snappy)
find_path(SNAPPY_INCLUDE_DIR NAMES snappy.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snappy DEFAULT_MSG SNAPPY_LIBRARY SNAPPY_INCLUDE_DIR)

if(SNAPPY_FOUND)
  add_library(Snappy::snappy UNKNOWN IMPORTED)
  set_target_properties(Snappy::snappy PROPERTIES
    IMPORTED_LOCATION "${SNAPPY_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE_DIR}"
  )
endif()
