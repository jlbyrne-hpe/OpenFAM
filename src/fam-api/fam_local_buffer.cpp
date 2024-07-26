/*
 * fam_local_buffer.cpp
 * Copyright (c) 2024 Hewlett Packard Enterprise Development, LP. All
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

#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "allocator/fam_allocator_client.h"
#include "common/fam_config_info.h"
#include "common/fam_internal.h"
#include "common/fam_libfabric.h"
#include "common/fam_ops.h"
#include "common/fam_ops_libfabric.h"
#include "common/fam_ops_shm.h"
#include "common/fam_options.h"
#include "fam/fam.h"
#include "fam/fam_exception.h"

using namespace std;

namespace openfam {

fam_local_buffer::fam_local_buffer(fam_local_buffer::Impl *pfbimpl) {
    this->pfbimpl_ = pfbimpl;
    this->start_ = pfbimpl->start;
    this->len_ = pfbimpl->len;
    this->putOnly_ = pfbimpl->putOnly;
    if (pfbimpl->mr)
        this->desc_ = fi_mr_desc(pfbimpl->mr);
    else
        this->desc_ = nullptr;
}

uint64_t fam_local_buffer::get_rKey(void) {
    if (pfbimpl_->mr)
        return fi_mr_key(pfbimpl_->mr);
    else
        return FI_KEY_NOTAVAIL;
}

size_t fam_local_buffer::get_epAddrLen(void) {
    return pfbimpl_->epAddrLen;
}

void *fam_local_buffer::get_epAddr(void) {
    return (void *)pfbimpl_->epAddr;
}

void fam_local_buffer::check_offset_and_len(size_t opOffset, size_t opLen) {
    size_t opEnd = opOffset + opLen;
    if (opEnd < opOffset || opOffset >= len_ || opEnd > len_)
        THROW_ERR_MSG(Fam_Datapath_Exception,
                      "offset or len out of bounds of registered memory");
}

void fam_local_buffer::check_buffer(void *opStartPtr, size_t opLen) {
    uintptr_t opStart = (uintptr_t)opStartPtr;
    uintptr_t opEnd = opStart + opLen;
    uintptr_t regEnd = start_ + len_;
    if (opEnd < opStart || opStart < start_ || opEnd > regEnd)
        THROW_ERR_MSG(Fam_Datapath_Exception,
                      "buffer out of bounds of registered memory");
}

fam_local_buffer::Impl::Impl(Fam_Ops *famOps, void *start,
			     uint64_t len, bool putOnly, bool remoteAccess) {
    this->famOps = famOps;
    this->start = (uintptr_t)start;
    this->len = len;
    this->mr = nullptr;
    this->epAddr = nullptr;
    this->epAddrLen = 0;
    this->putOnly = putOnly;
    famOps->register_buffer(this, remoteAccess);
}

fam_local_buffer::Impl::~Impl() {
    famOps->deregister_buffer(this);
}

} // namespace openfam
