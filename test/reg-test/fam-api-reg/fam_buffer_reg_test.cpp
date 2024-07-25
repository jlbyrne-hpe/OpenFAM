/*
 * fam_buffer_reg_test.cpp
 * Copyright (c) 2019 Hewlett Packard Enterprise Development, LP. All rights
 * reserved. Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
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
#include <fam/fam_exception.h>
#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <string.h>

#include <rdma/fabric.h>

#include <fam/fam.h>

#include "common/fam_test_config.h"

using namespace std;
using namespace openfam;

fam *my_fam;
Fam_Options fam_opts;

#define MYTYPE uint32_t

#define EXPECT_FAMEX(statement) EXPECT_THROW(statement, Fam_Exception)

static void checkOutOfBounds(Fam_Descriptor *item, fam_buffer *fb)
{
    size_t offset = fb->get_offset();
    size_t len = fb->get_len();
    size_t count = len / sizeof(MYTYPE) ;
    std::vector<uint64_t> indexesVec(count + 1, 0);
    uint64_t *indexes = &indexesVec[0];

    EXPECT_FAMEX(fb->set_offset(offset + len));
    EXPECT_EQ(offset, fb->get_offset());

    fam_buffer *fbOff = nullptr;
    EXPECT_FAMEX(fbOff = new fam_buffer(fb, len));
    EXPECT_EQ(fbOff, nullptr);

    // Check for out of bounds errors for all data transfer APIs.
    EXPECT_FAMEX(my_fam->fam_put_blocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_get_blocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_scatter_blocking(fb, item, count + 1, indexes,
                                              sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, item, count + 1, indexes,
                                             sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_scatter_blocking(fb, item, count + 1, 1, 2,
                                              sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, item, count + 1, 1, 2,
                                             sizeof(MYTYPE)));

    EXPECT_FAMEX(my_fam->fam_put_nonblocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_get_nonblocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, item, count + 1, indexes,
                                                 sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, item, count + 1, indexes,
                                                sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, item, count + 1, 1, 2,
                                                 sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, item, count + 1, 1, 2,
                                                 sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, item, count + 1, 1, 2,
                                                sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
}

template <typename T>
void checkAtomic(Fam_Descriptor *item, uint64_t offset, T startValue)
{
    T operand = 1;
    T oldValue;

    EXPECT_NO_THROW(my_fam->fam_add(item, offset, operand));
    EXPECT_NO_THROW(oldValue = my_fam->fam_fetch_add(item, offset, operand));
    EXPECT_EQ(oldValue, startValue + 1);
    EXPECT_NO_THROW(
        oldValue = my_fam->fam_compare_swap(item, offset, startValue,
                                            startValue + 3));
    EXPECT_EQ(oldValue, startValue + 2);
    EXPECT_NO_THROW(
        oldValue = my_fam->fam_compare_swap(item, offset, oldValue,
                                            startValue + 3));
    EXPECT_EQ(oldValue, startValue + 2);
    EXPECT_NO_THROW(oldValue = my_fam->fam_fetch_add(item, offset, (T)0));
    EXPECT_EQ(oldValue, startValue + 3);
}

// Test case 1 - All the fam_buffer tests.
TEST(FamBuffer, FamBufferSuccess) {
    Fam_Region_Descriptor *desc = nullptr;
    Fam_Descriptor *item = nullptr;
    const char *testRegion = get_uniq_str("test", my_fam);
    const char *firstItem = get_uniq_str("first", my_fam);
    size_t local1Count = 256;
    size_t local1Size = local1Count * sizeof(MYTYPE);
    size_t i;
    std::vector<MYTYPE> local1Vec(local1Count, 0);
    for (i = 0; i < local1Count; i++) {
        local1Vec[i] = (MYTYPE)i * 0x01010101U;
    }
    MYTYPE *local1 = &local1Vec[0];
    std::vector<uint64_t> indexes1Vec(local1Count, 0);
    std::vector<uint64_t> indexes2Vec(local1Count, 0);
    for (i = 0; i < local1Count; i++) {
        indexes1Vec[i] = i * 2;
        indexes2Vec[i] = i * 2 + 1;
    }
    uint64_t *indexes1 = &indexes1Vec[0];
    uint64_t *indexes2 = &indexes2Vec[0];
    size_t local2Count = local1Count * 2;
    size_t local2Size = local2Count * sizeof(MYTYPE);
    std::vector<MYTYPE> local2Vec(local2Count, 0);
    MYTYPE *local2 = &local2Vec[0];
    std::vector<MYTYPE> zeroesVec(local2Count, 0);
    MYTYPE *zeroes = &zeroesVec[0];

    EXPECT_NO_THROW(
        desc = my_fam->fam_create_region(testRegion, 8192, 0777, NULL));
    EXPECT_NE(nullptr, desc);

    // Allocating data items in the created region
    EXPECT_NO_THROW(item = my_fam->fam_allocate(firstItem, local2Size, 0777,
                                                desc));
    EXPECT_NE(nullptr, item);
    // Test fam_buffers
    fam_buffer *fbLocal1 = nullptr;
    fam_buffer *fbLocal2 = nullptr;
    fam_buffer *fbLocal2Offset = nullptr;
    fam_buffer *fbLocal2Offset2 = nullptr;
    fam_buffer *fbLocal2ReadOnly = nullptr;
    fam_buffer *fbZeroes = nullptr;

    EXPECT_NO_THROW(fbLocal1 =
                    my_fam->fam_buffer_register((void *)local1, local1Size));
    EXPECT_EQ(fbLocal1->get_start(), (uintptr_t)local1);
    EXPECT_EQ(fbLocal1->get_epAddr(), nullptr);
    EXPECT_EQ(fbLocal1->get_epAddrLen(), 0);
    EXPECT_EQ(fbLocal1->get_rKey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbLocal1);

    EXPECT_NO_THROW(fbLocal2 =
                    my_fam->fam_buffer_register((void *)local2, local2Size,
                                                false, true));
    EXPECT_EQ(fbLocal2->get_start(), (uintptr_t)local2);
    EXPECT_NE(fbLocal2->get_epAddr(), nullptr);
    EXPECT_NE(fbLocal2->get_epAddrLen(), 0);
    EXPECT_NE(fbLocal2->get_rKey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbLocal2);

    // Derived from fbLocal2.
    EXPECT_NO_THROW(fbLocal2Offset = new fam_buffer(fbLocal2, local2Size - 1));
    EXPECT_EQ(fbLocal2Offset->get_offset(), local2Size - 1);
    // Set offset to something useful for test;
    EXPECT_NO_THROW(fbLocal2Offset->set_offset(sizeof(MYTYPE)));
    EXPECT_EQ(fbLocal2Offset->get_offset(), sizeof(MYTYPE));
    EXPECT_EQ((uintptr_t)local2 + sizeof(MYTYPE), fbLocal2Offset->get_start());
    EXPECT_EQ(fbLocal2Offset->get_epAddr(), fbLocal2->get_epAddr());
    EXPECT_EQ(fbLocal2Offset->get_epAddrLen(), fbLocal2->get_epAddrLen());
    EXPECT_EQ(fbLocal2Offset->get_rKey(), fbLocal2->get_rKey());

    checkOutOfBounds(item, fbLocal2Offset);

    // Derived from fbLocal2Offset
    EXPECT_NO_THROW(fbLocal2Offset2 = new fam_buffer(fbLocal2Offset,
						     sizeof(MYTYPE)));
    EXPECT_EQ(fbLocal2Offset2->get_offset(), sizeof(MYTYPE) * 2);
    EXPECT_EQ((uintptr_t)local2 + sizeof(MYTYPE) * 2,
	      fbLocal2Offset2->get_start());
    EXPECT_EQ(fbLocal2Offset2->get_epAddr(), fbLocal2->get_epAddr());
    EXPECT_EQ(fbLocal2Offset2->get_epAddrLen(), fbLocal2->get_epAddrLen());
    EXPECT_EQ(fbLocal2Offset2->get_rKey(),fbLocal2->get_rKey());

    // Read-only buffer registration for same range
    // cxi ignores local access bits; honors remote bits.
    EXPECT_NO_THROW(fbLocal2ReadOnly =
		    my_fam->fam_buffer_register((void *)local2, local2Size,
                                                true));
    EXPECT_EQ(fbLocal2ReadOnly->get_start(), (uintptr_t)local2);
    EXPECT_EQ(fbLocal2ReadOnly->get_epAddr(), nullptr);
    EXPECT_EQ(fbLocal2ReadOnly->get_epAddrLen(), 0);
    EXPECT_EQ(fbLocal2ReadOnly->get_rKey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbLocal2ReadOnly);

    EXPECT_NO_THROW(fbZeroes =
		    my_fam->fam_buffer_register((void *)zeroes, local2Size));
    EXPECT_EQ((uintptr_t)zeroes, fbZeroes->get_start());
    EXPECT_EQ(fbZeroes->get_epAddr(), nullptr);
    EXPECT_EQ(fbZeroes->get_epAddrLen(), 0);
    EXPECT_EQ(fbZeroes->get_rKey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbZeroes);

    // put and get, blocking
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbLocal1, item, 0, local1Size));
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_get_blocking(fbLocal2ReadOnly, item, 0,
                                          local1Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local1Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbLocal2Offset, item, 0,
                                             local1Size));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2Offset, item, 0,
                                             local1Size));
    EXPECT_EQ(local2[0], 0);
    for (i = 1; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_EQ(local2[i], 0);

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

     // put and get, nonblocking
    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fbLocal1, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_get_nonblocking(fbLocal2ReadOnly, item, 0,
					     local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fbLocal2, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fbLocal2Offset, item, 0,
                                                local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fbLocal2Offset, item, 0,
                                                local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_EQ(local2[0], 0);
    for (i = 1; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_EQ(local2[i], 0);

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // indexed scatter and gather, blocking
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal1, item, local1Count,
                                                 indexes1, sizeof(MYTYPE)));
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fbLocal2ReadOnly, item,
                                             local1Count, indexes1,
                                             sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, item, local1Count,
                                                indexes1, sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2Offset, item,
                                                local1Count, indexes1,
                                                sizeof(MYTYPE)));
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal2Offset, item,
                                                 local1Count, indexes2,
                                                 sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // indexed scatter and gather, nonblocking
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal1, item, local1Count,
                                                    indexes1, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fbLocal2ReadOnly, item,
						local1Count, indexes1,
						sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, item, local1Count,
                                                   indexes1, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2Offset, item,
                                                   local1Count, indexes1,
                                                   sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal2Offset, item,
                                                    local1Count, indexes2,
                                                    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // strided scatter and gather, blocking
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal1, item, local1Count,
                                                 0, 2, sizeof(MYTYPE)));
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fbLocal2ReadOnly, item,
                                             local1Count, 0, 2,
                                             sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, item, local1Count,
                                                0, 2, sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2Offset, item,
                                                local1Count, 0, 2,
                                                sizeof(MYTYPE)));
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal2Offset, item,
                                                 local1Count, 1, 2,
                                                 sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // strided scatter and gather, blocking
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal1, item, local1Count,
                                                 0, 2, sizeof(MYTYPE)));
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fbLocal2ReadOnly, item,
                                             local1Count, 0, 2,
                                             sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, item, local1Count,
                                                0, 2, sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2Offset, item,
                                                local1Count, 0, 2,
                                                sizeof(MYTYPE)));
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal2Offset, item,
                                                 local1Count, 1, 2,
                                                 sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // strided scatter and gather, nonblocking
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal1, item, local1Count,
                                                    0, 2, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fbLocal2ReadOnly, item,
						local1Count, 0, 2,
                                                sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, item, local1Count,
                                                   0, 2, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2Offset, item,
                                                   local1Count, 0, 2,
                                                   sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal2Offset, item,
						    local1Count, 1, 2,
						    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // The changes for fam_buffer support in libfabric atomics are internal
    // to libopenfam and were made in the common fabric_atomic(),
    // fabric_fetch_atomic(), and fabric_compare_atomic() routines.
    // Just going to call the APIs necessary to test each one.
    checkAtomic<int32_t>(item, 0, 0);
    checkAtomic<int64_t>(item, 0, 3);
    int128_t oldValue = 6;
    int128_t newValue = 0x0102030405060708;
    newValue <<= 64;
    newValue |= 0x090A0B0C0D0E0F00;
    EXPECT_NO_THROW(oldValue = my_fam->fam_compare_swap(item, 0, oldValue,
                                                        newValue));
    EXPECT_EQ(oldValue, 6);
    oldValue = my_fam->fam_fetch_int128(item, 0);
    EXPECT_EQ(oldValue, newValue);

    // Deleting remaining fam_buffers unregisters the memory.
    delete fbLocal1;
    delete fbLocal2;
    delete fbLocal2Offset;
    delete fbLocal2Offset2;
    delete fbLocal2ReadOnly;
    delete fbZeroes;

    EXPECT_NO_THROW(my_fam->fam_deallocate(item));
    EXPECT_NO_THROW(my_fam->fam_destroy_region(desc));

    delete item;
    delete desc;

    free((void *)testRegion);
    free((void *)firstItem);
}

int main(int argc, char **argv) {
    int ret;
    ::testing::InitGoogleTest(&argc, argv);

    my_fam = new fam();

    init_fam_options(&fam_opts);

    EXPECT_NO_THROW(my_fam->fam_initialize("default", &fam_opts));

    ret = RUN_ALL_TESTS();

    EXPECT_NO_THROW(my_fam->fam_finalize("default"));

    return ret;
}
