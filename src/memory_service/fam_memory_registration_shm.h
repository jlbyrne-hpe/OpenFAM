/*
 * fam_memory_registration_shm.h
 * Copyright (c) 2020 Hewlett Packard Enterprise Development, LP. All
 * rights reserved. Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 *    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *    INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * See https://spdx.org/licenses/BSD-3-Clause
 *
 */

#ifndef FAM_MEMORY_REGISTRATION_SHM_H
#define FAM_MEMORY_REGISTRATION_SHM_H

#include <iostream>
#include <map>
#include <thread>
#include <unistd.h>

#include "memory_service/fam_memory_registration.h"

#include "common/fam_internal.h"
#include "common/fam_internal_exception.h"
#include "common/fam_options.h"

#include <boost/atomic.hpp>

using namespace std;

namespace openfam {

class Fam_Memory_Registration_SHM : public Fam_Memory_Registration {
  public:
    Fam_Memory_Registration_SHM() {}

    ~Fam_Memory_Registration_SHM() {}

    void reset_profile() {}

    void dump_profile() {}

    uint64_t generate_access_key(uint64_t regionId, uint64_t dataitemId,
                                 bool permission) {
        return 0;
    }

    void deregister_memory(uint64_t regionId, uint64_t offset) {}

    void deregister_region_memory(uint64_t regionId) {}

    uint64_t register_memory(uint64_t regionId, uint64_t offset, void *base,
                             uint64_t size, bool rwFlag) {
        if (rwFlag)
            return FAM_WRITE_KEY_SHM | FAM_READ_KEY_SHM;
        else
            return FAM_READ_KEY_SHM;
    }

    void register_fence_memory() {}

    void deregister_fence_memory() {}

    size_t get_addr_size() { return 0; }

    void *get_addr() { return NULL; }

    bool is_base_require() { return true; }
};

} // namespace openfam
#endif
