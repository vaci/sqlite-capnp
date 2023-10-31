#pragma once
// Copyright (c) 2023 Vaci Koblizek.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include <sqlite3.h>
#include <capnp/schema.h>
#include <kj/string.h>

namespace sqlcap {

kj::String create(capnp::StructSchema schema);
kj::String insert(capnp::StructSchema schema);

}
