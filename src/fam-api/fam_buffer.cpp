/*
 * fam_buffer.cpp
 * Copyright (c) 2019-2023 Hewlett Packard Enterprise Development, LP. All
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

fam_buffer::fam_buffer(fam_buffer *orig, size_t off) {
    this->pfbimpl = orig->pfbimpl;
    this->off_ = orig->off_ + off;
    this->start_ = orig->start_;
    this->len_ = orig->len_;
    this->desc_ = orig->desc_;
    this->readOnly_ = orig->readOnly_;
    if (this->off_ >= this->len_)
        THROW_ERR_MSG(Fam_Datapath_Exception,
                      "offset out of bounds of registered memory");
}

fam_buffer::fam_buffer(fam_buffer::Impl *pfbimpl) {
    this->pfbimpl = std::shared_ptr<fam_buffer::Impl>(pfbimpl);
    this->off_ = 0;
    this->start_ = pfbimpl->regStart;
    this->len_ = pfbimpl->regLen;
    this->readOnly_ = pfbimpl->readOnly;
    if (pfbimpl->mr)
        this->desc_ = fi_mr_desc(pfbimpl->mr);
    else
        this->desc_ = nullptr;
}

void fam_buffer::set_offset(size_t off) {
    if (off >= this->len_)
        THROW_ERR_MSG(Fam_Datapath_Exception,
                      "offset out of bounds of registered memory");
    this->off_ = off;
}

uint64_t fam_buffer::get_rKey(void) {
    if (pfbimpl->mr)
        return fi_mr_key(pfbimpl->mr);
    else
        return FI_KEY_NOTAVAIL;
}

size_t fam_buffer::get_epAddrLen(void) {
    return pfbimpl->epAddrLen;
}

void *fam_buffer::get_epAddr(void) {
    return (void *)pfbimpl->epAddr;
}

void fam_buffer::check_bounds(uintptr_t opStart, size_t opLen) {
    uintptr_t opEnd = opStart + opLen;
    uintptr_t regEnd = start_ + len_;
    if (opStart < start_ + off_ || opEnd > regEnd || opEnd < opStart)
        THROW_ERR_MSG(Fam_Datapath_Exception,
                      "buffer out of bounds of registered memory");
}

void fam_buffer::check_bounds(void *start, size_t len) {
    check_bounds((uintptr_t)start, len);
}

fam_buffer::Impl::Impl(Fam_Ops *famOps, void *start,
                       uint64_t len,  bool readOnly, bool remoteAccess) {
    this->famOps = famOps;
    this->regStart = (uintptr_t)start;
    this->regLen = len;
    this->mr = nullptr;
    this->epAddr = nullptr;
    this->epAddrLen = 0;
    this->readOnly = readOnly;
    famOps->register_buffer(this, remoteAccess);
}

fam_buffer::Impl::~Impl() {
    famOps->deregister_buffer(this);
}

} // namespace openfam
