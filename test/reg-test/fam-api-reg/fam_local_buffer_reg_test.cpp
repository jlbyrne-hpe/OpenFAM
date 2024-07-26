/*
 * fam_local_buffer_reg_test.cpp
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

static void checkPutGetNoThrow(Fam_Descriptor *item, fam_local_buffer *fb,
                               size_t localOffset, size_t len)
{
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fb, localOffset, item, 0, len));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fb, localOffset, item, 0, len));

    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fb, localOffset, item, 0, len));
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fb, localOffset, item, 0, len));
    EXPECT_NO_THROW(my_fam->fam_quiet());
}

static void checkScatterGatherNoThrow(
        Fam_Descriptor *item, fam_local_buffer *fb, uint64_t *indexes,
        size_t localOffset, size_t count)
{
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fb, localOffset, item, count,
                                                 indexes, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fb, localOffset, item, count,
                                                indexes, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fb, localOffset, item, count,
                                                 0, 2, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fb, localOffset, item, count,
                                                0, 2, sizeof(MYTYPE)));

    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fb, localOffset, item,
                                                    count, indexes,
                                                    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fb, localOffset, item, count,
                                                   indexes, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fb, localOffset, item,
                                                    count, 0, 2,
                                                    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fb, localOffset, item, count,
                                                   0, 2, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
}

static void checkPutGetFamEx(Fam_Descriptor *item, fam_local_buffer *fb,
                             size_t localOffset, size_t len)
{
    EXPECT_FAMEX(my_fam->fam_put_blocking(fb, localOffset, item, 0, len));
    EXPECT_FAMEX(my_fam->fam_get_blocking(fb, localOffset, item, 0, len));

    EXPECT_FAMEX(my_fam->fam_put_nonblocking(fb, localOffset, item, 0, len));
    EXPECT_FAMEX(my_fam->fam_get_nonblocking(fb, localOffset, item, 0, len));
    EXPECT_NO_THROW(my_fam->fam_quiet());
}

static void checkScatterGatherFamEx(
        Fam_Descriptor *item, fam_local_buffer *fb,uint64_t *indexes,
        size_t localOffset, size_t count)
{
    EXPECT_FAMEX(my_fam->fam_scatter_blocking(fb, localOffset, item, count,
                                              indexes, sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, localOffset, item, count,
                                             indexes, sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_scatter_blocking(fb, localOffset, item, count,
                                              0, 2, sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, localOffset, item, count,
                                             0, 2, sizeof(MYTYPE)));

    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, localOffset, item,
                                                 count, indexes,
                                                 sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, localOffset, item, count,
                                                indexes, sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, localOffset, item,
                                                 count, 0, 2, sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, localOffset, item, count,
                                                0, 2, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
}

static void checkLocalOutOfBounds(Fam_Descriptor *item, fam_local_buffer *fb,
                                  bool outOfBoundsOnly)
{
    size_t len = fb->get_len();
    size_t count = len / sizeof(MYTYPE) ;
    std::vector<uint64_t> indexesVec(count + 1, 0);
    uint64_t *indexes = &indexesVec[0];
    for (size_t i = 0; i <= count; i++)
        indexes[i] = i;

    // Check for out of bounds errors for the fam_local_buffer, not the
    // data item; the sum of the localOffset and the bytes transferred
    // cannot exceed the sizeof of the fam_local_buffer.
    if (!outOfBoundsOnly) {
        checkPutGetNoThrow(item, fb, 0, len);
        checkPutGetNoThrow(item, fb, 1, len - 1);
        checkScatterGatherNoThrow(item, fb, indexes, 0, count);
        checkScatterGatherNoThrow(item, fb, indexes, sizeof(MYTYPE), count - 1);
    }

    checkPutGetFamEx(item, fb, 1, len);
    checkPutGetFamEx(item, fb, 0, len + 1);
    checkScatterGatherFamEx(item, fb, indexes, sizeof(MYTYPE), count);
    checkScatterGatherFamEx(item, fb, indexes, 0, count + 1);
}

static void checkLocalPutOnly(Fam_Descriptor *item, fam_local_buffer *fb)
{
    size_t len = fb->get_len();
    size_t count = len / sizeof(MYTYPE) ;
    std::vector<uint64_t> indexesVec(count, 0);
    uint64_t *indexes = &indexesVec[0];
    for (size_t i = 0; i < count; i++)
        indexes[i] = i;

    EXPECT_NO_THROW(my_fam->fam_put_blocking(fb, 0, item, 0, len));
    EXPECT_FAMEX(my_fam->fam_get_blocking(fb, 0, item, 0, len));

    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fb, 0, item, 0, len));
    EXPECT_FAMEX(my_fam->fam_get_nonblocking(fb, 0, item, 0, len));
    EXPECT_NO_THROW(my_fam->fam_quiet());

    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fb, 0, item, count,
                                                 indexes, sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, 0, item, count,
                                             indexes, sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fb, 0, item, count,
                                                 0, 1, sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, 0, item, count,
                                             0, 1, sizeof(MYTYPE)));

    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fb, 0, item, count, indexes,
                                                    sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, 0, item, count, indexes,
                                                sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fb, 0, item, count, 0, 2,
                                                    sizeof(MYTYPE)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, 0, item, count, 0, 2,
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

// Test case 1 - All the fam_local_buffer tests.
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

    EXPECT_NO_THROW(desc = my_fam->fam_create_region(testRegion, local2Size * 5,
                                                     0777, NULL));
    EXPECT_NE(nullptr, desc);

    // Allocating data items in the created region
    EXPECT_NO_THROW(item = my_fam->fam_allocate(firstItem, local2Size * 2,
                                                0777, desc));
    EXPECT_NE(nullptr, item);
    // Test fam_local_buffers
    fam_local_buffer *fbLocal1 = nullptr;
    fam_local_buffer *fbLocal2 = nullptr;
    fam_local_buffer *fbZeroes = nullptr;

    EXPECT_NO_THROW(fbLocal1 = my_fam->fam_local_buffer_register(
                        (void *)local1, local1Size));
    EXPECT_EQ(fbLocal1->get_start(), (uintptr_t)local1);
    EXPECT_EQ(fbLocal1->get_len(), local1Size);
    EXPECT_FALSE(fbLocal1->get_putOnly());
    EXPECT_EQ(fbLocal1->get_epAddr(), nullptr);
    EXPECT_EQ(fbLocal1->get_epAddrLen(), 0);
    EXPECT_EQ(fbLocal1->get_rKey(), FI_KEY_NOTAVAIL);

    checkLocalOutOfBounds(item, fbLocal1, true);

    EXPECT_NO_THROW(fbLocal2 = my_fam->fam_local_buffer_register(
                        (void *)local2, local2Size, false, true));
    EXPECT_EQ(fbLocal2->get_start(), (uintptr_t)local2);
    EXPECT_EQ(fbLocal2->get_len(), local2Size);
    EXPECT_FALSE(fbLocal2->get_putOnly());
    EXPECT_NE(fbLocal2->get_epAddr(), nullptr);
    EXPECT_NE(fbLocal2->get_epAddrLen(), 0);
    EXPECT_NE(fbLocal2->get_rKey(), FI_KEY_NOTAVAIL);

    checkLocalOutOfBounds(item, fbLocal2, false);

    // Put-only buffer to reset things.
    EXPECT_NO_THROW(fbZeroes = my_fam->fam_local_buffer_register(
                        (void *)zeroes, local2Size, true));
    EXPECT_EQ((uintptr_t)zeroes, fbZeroes->get_start());
    EXPECT_EQ(fbZeroes->get_len(), local2Size);
    EXPECT_TRUE(fbZeroes->get_putOnly());
    EXPECT_EQ(fbZeroes->get_epAddr(), nullptr);
    EXPECT_EQ(fbZeroes->get_epAddrLen(), 0);
    EXPECT_EQ(fbZeroes->get_rKey(), FI_KEY_NOTAVAIL);

    checkLocalOutOfBounds(item, fbZeroes, true);
    checkLocalPutOnly(item, fbZeroes);

    // put and get, blocking
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbLocal1, 0, item, 0, local1Size));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local1Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);

    // Offset the fam_local_buffer by sizeof(MYTYPE)
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbLocal2, sizeof(MYTYPE),
                                             item, 0, local1Size));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, sizeof(MYTYPE),
                                             item, 0, local1Size));
    EXPECT_EQ(local2[0], 0);
    for (i = 1; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_EQ(local2[i], 0);

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, 0, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

     // put and get, nonblocking
    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fbLocal1, 0,
                                                item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fbLocal2, 0,
                                                item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);

    // Offset the fam_local_buffer by sizeof(MYTYPE)
    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fbLocal2, sizeof(MYTYPE),
                                                item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fbLocal2, sizeof(MYTYPE),
                                                item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_EQ(local2[0], 0);
    for (i = 1; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_EQ(local2[i], 0);

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, 0, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // indexed scatter and gather, blocking
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal1, 0,
                                                 item, local1Count, indexes1,
                                                 sizeof(MYTYPE)));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, 0,
                                                item, local1Count, indexes1,
                                                sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Offset the fam_local_buffer by sizeof(MYTYPE)
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, sizeof(MYTYPE),
                                                item, local1Count, indexes1,
                                                sizeof(MYTYPE)));
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal2, sizeof(MYTYPE),
                                                 item, local1Count, indexes2,
                                                 sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, 0, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // indexed scatter and gather, nonblocking
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal1, 0,
                                                    item, local1Count, indexes1,
                                                    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, 0,
                                                   item, local1Count, indexes1,
                                                   sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Offset the fam_local_buffer by sizeof(MYTYPE)
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, sizeof(MYTYPE),
                                                   item, local1Count, indexes1,
                                                   sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal2, sizeof(MYTYPE),
                                                    item, local1Count, indexes2,
                                                    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, 0, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // strided scatter and gather, blocking
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal1, 0,
                                                 item, local1Count, 0, 2,
                                                 sizeof(MYTYPE)));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, 0,
                                                item, local1Count, 0, 2,
                                                sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Offset the fam_local_buffer by sizeof(MYTYPE)
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, sizeof(MYTYPE),
                                                item, local1Count, 0, 2,
                                                sizeof(MYTYPE)));
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal2, sizeof(MYTYPE),
                                                 item, local1Count, 1, 2,
                                                 sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, 0, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // strided scatter and gather, blocking
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal1, 0,
                                                 item, local1Count, 0, 2,
                                                 sizeof(MYTYPE)));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, 0,
                                                item, local1Count, 0, 2,
                                                sizeof(MYTYPE)));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Offset the fam_local_buffer by sizeof(MYTYPE)
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, sizeof(MYTYPE),
                                                item, local1Count, 0, 2,
                                                sizeof(MYTYPE)));
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal2, sizeof(MYTYPE),
                                                 item, local1Count, 1, 2,
                                                 sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, 0, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // strided scatter and gather, nonblocking
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal1, 0,
                                                    item, local1Count, 0, 2,
                                                    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, 0,
                                                   item, local1Count, 0, 2,
                                                   sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++) {
        EXPECT_EQ(local2[i * 2], local1[i]);
        EXPECT_EQ(local2[i * 2 + 1], 0);
    }

    // Offset the fam_local_buffer by sizeof(MYTYPE)
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, sizeof(MYTYPE),
                                                   item, local1Count, 0, 2,
                                                   sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_EQ(local2[0], local1[0]);
    for (i = 1; i < local1Count + 1; i++)
        EXPECT_EQ(local2[i], local1[i - 1]);
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal2, sizeof(MYTYPE),
                                                    item, local1Count, 1, 2,
                                                    sizeof(MYTYPE)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local2Count; i++) {
        EXPECT_EQ(local2[i], local1[i / 2]);
    }

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, 0, item, 0, local2Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, 0, item, 0, local2Size));
    for (i = 0; i < local1Count; i++)
        EXPECT_EQ(local2[i], 0);

    // The changes for fam_local_buffer support in libfabric atomics are
    // internal to libopenfam and were made in the common fabric_atomic(),
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

    // Deleting fam_local_buffers unregisters the memory, not the memory.
    delete fbLocal1;
    delete fbLocal2;
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
