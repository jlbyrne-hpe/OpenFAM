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

#define EXPECT_FAMEX(statement) EXPECT_THROW(statement, Fam_Exception)

static void checkOutOfBounds(Fam_Descriptor *item, fam_buffer *fb)
{
    size_t offset = fb->get_offset();
    size_t len = fb->get_len();
    size_t nInts = len / sizeof(int);
    uint64_t indexes[nInts + 1] = { 0 };

    EXPECT_FAMEX(fb->set_offset(offset + len));
    EXPECT_EQ(offset, fb->get_offset());

    fam_buffer *fbOff = nullptr;
    EXPECT_FAMEX(fbOff = new fam_buffer(fb, len));
    EXPECT_EQ(fbOff, nullptr);

    // Check for out of bounds errors for all data transfer APIs.
    EXPECT_FAMEX(my_fam->fam_put_blocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_get_blocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_scatter_blocking(fb, item, nInts + 1, indexes,
                                              sizeof(int)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, item, nInts + 1, indexes,
                                             sizeof(int)));
    EXPECT_FAMEX(my_fam->fam_scatter_blocking(fb, item, nInts + 1, 1, 2,
                                              sizeof(int)));
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fb, item, nInts + 1, 1, 2,
                                             sizeof(int)));

    EXPECT_FAMEX(my_fam->fam_put_nonblocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_get_nonblocking(fb, item, 0, len + 1));
    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, item, nInts + 1, indexes,
                                                 sizeof(int)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, item, nInts + 1, indexes,
                                                sizeof(int)));
    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, item, nInts + 1, 1, 2,
                                                 sizeof(int)));
    EXPECT_FAMEX(my_fam->fam_scatter_nonblocking(fb, item, nInts + 1, 1, 2,
                                                 sizeof(int)));
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fb, item, nInts + 1, 1, 2,
                                                sizeof(int)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
}

// Test case 1 - put get test.
TEST(FamBuffer, FamBufferSuccess) {
    Fam_Region_Descriptor *desc;
    Fam_Descriptor *item;
    const char *testRegion = get_uniq_str("test", my_fam);
    const char *firstItem = get_uniq_str("first", my_fam);
    // allocate local memory to receive 20 elements
    int local1[] = {
        0x01010101, 0x02020202, 0x03030303, 0x04040404,
        0x05050505, 0x06060606, 0x07070707, 0x08080808
    };
    size_t local1Size = sizeof(local1);
    size_t local1Ints = local1Size / sizeof(int);
    uint64_t indexes[] = { 7, 6, 5, 4, 3, 2, 1, 0 };
    EXPECT_EQ(local1Size, sizeof(indexes));
    int zeroes[] = { 0, 0, 0, 0, 0, 0, 0, 0 };
    EXPECT_EQ(local1Size, sizeof(zeroes));
    size_t local2Ints = local1Ints * 2;
    size_t local2Size = local2Ints * sizeof(int);
    char *local2 = (char *)malloc(local2Size);
    size_t i;

    EXPECT_NO_THROW(
        desc = my_fam->fam_create_region(testRegion, 8192, 0777, NULL));
    EXPECT_NE(nullptr, desc);

    // Allocating data items in the created region
    EXPECT_NO_THROW(item = my_fam->fam_allocate(firstItem, 1024, 0777, desc));
    EXPECT_NE(nullptr, item);

    // Test fam_buffers
    fam_buffer *fbLocal1 = nullptr;
    fam_buffer *fbLocal2 = nullptr;
    fam_buffer *fbLocal2Offset = nullptr;
    fam_buffer *fbLocal2ReadOnly = nullptr;
    fam_buffer *fbZeroes = nullptr;

    EXPECT_NO_THROW(fbLocal1 =
                    my_fam->fam_buffer_register((void *)local1, local1Size));
    EXPECT_EQ(fbLocal1->get_start(), (uintptr_t)local1);
    EXPECT_EQ(fbLocal1->get_ep_addr(), nullptr);
    EXPECT_EQ(fbLocal1->get_ep_addr_len(), 0);
    EXPECT_EQ(fbLocal1->get_rkey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbLocal1);

    EXPECT_NO_THROW(fbLocal2 =
                    my_fam->fam_buffer_register((void *)local2, local2Size,
                                                false, true));
    EXPECT_EQ(fbLocal2->get_start(), (uintptr_t)local2);
    EXPECT_NE(fbLocal2->get_ep_addr(), nullptr);
    EXPECT_NE(fbLocal2->get_ep_addr_len(), 0);
    EXPECT_NE(fbLocal2->get_rkey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbLocal2);

    // Derived from fbLocal2.
    EXPECT_FAMEX(fbLocal2Offset = new fam_buffer(fbLocal2, local2Size - 1));
    EXPECT_EQ(fbLocal2Offset->get_offset(), local2Size - 1);
    // Set offset to something useful for test;
    EXPECT_NO_THROW(fbLocal2Offset->set_offset(sizeof(int)));
    EXPECT_EQ(fbLocal2Offset->get_offset(), sizeof(int));
    EXPECT_EQ((uintptr_t)local2 + sizeof(int), fbLocal2Offset->get_start());
    EXPECT_NE(fbLocal2Offset->get_ep_addr(), fbLocal2->get_ep_addr());
    EXPECT_NE(fbLocal2Offset->get_ep_addr_len(), fbLocal2->get_ep_addr_len());
    EXPECT_NE(fbLocal2Offset->get_rkey(),fbLocal2->get_rkey());

    checkOutOfBounds(item, fbLocal2Offset);

    // Readonly buffer registration for same range
    EXPECT_NO_THROW(fbLocal2ReadOnly =
		    my_fam->fam_buffer_register((void *)local2, local2Size,
                                                true));
    EXPECT_EQ(fbLocal2ReadOnly->get_start(), (uintptr_t)local2);
    EXPECT_EQ(fbLocal2ReadOnly->get_ep_addr(), nullptr);
    EXPECT_EQ(fbLocal2ReadOnly->get_ep_addr_len(), 0);
    EXPECT_EQ(fbLocal2ReadOnly->get_rkey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbLocal2ReadOnly);

    EXPECT_NO_THROW(fbZeroes =
		    my_fam->fam_buffer_register((void *)zeroes, local1Size));
    EXPECT_EQ((uintptr_t)zeroes, fbZeroes->get_start());
    EXPECT_EQ(fbZeroes->get_ep_addr(), nullptr);
    EXPECT_EQ(fbZeroes->get_ep_addr_len(), 0);
    EXPECT_EQ(fbZeroes->get_rkey(), FI_KEY_NOTAVAIL);

    checkOutOfBounds(item, fbZeroes);

    // put and get, blocking
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbLocal1, item, 0, local1Size));
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_get_blocking(fbLocal2ReadOnly, item, 0,
                                          local1Size));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local1Size));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], local1[i]);

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbLocal2Offset, item, 0,
                                             local1Size));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local1Size));
    for (i = 0; i < local1Ints - 1; i++)
        EXPECT_EQ(local2[i], local1[i + 1]);
    EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2Offset, item, 0,
                                             local1Size));
    for (i = 0; i < local1Ints - 2; i++)
        EXPECT_EQ(local2[i], local1[i + 2]);
    EXPECT_EQ(local2[i], 0);
    EXPECT_EQ(local2[i + 1], 0);

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local1Size));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], 0);

    // put and get, nonblocking
    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fbLocal1, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_get_nonblocking(fbLocal2ReadOnly, item, 0,
                                             local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fbLocal2, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], local1[i]);

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_put_nonblocking(fbLocal2Offset, item, 0,
                                                local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fbLocal2, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints - 1; i++)
        EXPECT_EQ(local2[i], local1[i + 1]);
    EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_get_nonblocking(fbLocal2Offset, item, 0,
                                                local1Size));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints - 2; i++)
        EXPECT_EQ(local2[i], local1[i + 2]);
    EXPECT_EQ(local2[i], 0);
    EXPECT_EQ(local2[i + 1], 0);

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local1Size));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], 0);

    // scatter and gather, blocking
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal1, item, local1Ints,
                                                 indexes, sizeof(int)));
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_gather_blocking(fbLocal2ReadOnly, item, local1Ints,
                                             indexes, sizeof(int)));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, item, local1Ints,
                                                indexes, sizeof(int)));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2Offset, item, 0,
                                             local1Size));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i + 1], local1[local1Ints - i - 1]);

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_scatter_blocking(fbLocal2Offset, item,
                                                 local1Ints, indexes,
                                                 sizeof(int)));
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2, item, local1Ints,
                                                indexes, sizeof(int)));
    for (i = 0; i < local1Ints - 1; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_blocking(fbLocal2Offset, item,
                                                local1Ints, indexes,
                                                sizeof(int)));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i + 1], local1[i]);

    // Reset front of item to zeroes
    EXPECT_NO_THROW(my_fam->fam_put_blocking(fbZeroes, item, 0, local1Size));
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2, item, 0, local1Size));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], 0);

    // scatter and gather, nonblocking
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal1, item, local1Ints,
                                                    indexes, sizeof(int)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    // ReadOnly fam_buffer should fail
    EXPECT_FAMEX(my_fam->fam_gather_nonblocking(fbLocal2ReadOnly, item,
                                                local1Ints, indexes,
                                                sizeof(int)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, item, local1Ints,
                                                   indexes, sizeof(int)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_NO_THROW(my_fam->fam_get_blocking(fbLocal2Offset, item, 0,
                                             local1Size));
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i + 1], local1[local1Ints - i - 1]);

    // Try offset buffer
    EXPECT_NO_THROW(my_fam->fam_scatter_nonblocking(fbLocal2Offset, item,
                                                    local1Ints, indexes,
                                                    sizeof(int)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    memset(local2, 0, local2Size);
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2, item, local1Ints,
                                                   indexes, sizeof(int)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints - 1; i++)
        EXPECT_EQ(local2[i], local1[i]);
    EXPECT_EQ(local2[i], 0);
    EXPECT_NO_THROW(my_fam->fam_gather_nonblocking(fbLocal2Offset, item,
                                                   local1Ints, indexes,
                                                   sizeof(int)));
    EXPECT_NO_THROW(my_fam->fam_quiet());
    for (i = 0; i < local1Ints; i++)
        EXPECT_EQ(local2[i + 1], local1[i]);

    // Deleting remaining fam_buffers unregisters the memory.
    delete fbLocal1;
    delete fbLocal2;
    delete fbZeroes;

    EXPECT_NO_THROW(my_fam->fam_deallocate(item));
    EXPECT_NO_THROW(my_fam->fam_destroy_region(desc));

    delete item;
    delete desc;

    free((void *)local2);
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
