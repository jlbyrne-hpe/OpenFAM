/*
* test/microbench/fam-api-mb/baseline_test.cpp
* ./fam_microbench_multiple_di_in_single_region <data_item_size> <num_dataitems> <num_io_iters> <data_transfer_size> <num_msrv> <nodesperPE> <interlv_sz>
* Copyright () 2022 Hewlett Packard Enterprise Development, LP. All rights
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
#include <string>
#include  <unistd.h>
#include <sched.h>
#include <stdlib.h>
#include <chrono>
#include  <cstdlib>
#include <fam/fam.h>

#include "cis/fam_cis_client.h"
#include "common/fam_test_config.h"
#include "common/fam_libfabric.h"
#include <time.h>

#define BIG_REGION_SIZE (2147483648ULL * 128)
#define TEST_PROFILE 1

using namespace std::chrono;

using namespace std;
using namespace openfam;
using namespace chrono;

uint64_t gDataSize = 1048576;
uint64_t INTERLEAVING_SIZE = 1048576;
int sizes[] = {64,128,256,512,1024,2048,4096,8192,16384,65536,131072,262144,524288,1048576,2097152,4194304};
int api_val;
std::map<std::string, int> apis;
long ppn;
uint64_t buf_sz;
long wait_time;
long iters;
long num_cluster_nodes;
char *api_type;
char *device;
char hostname[100];
int *myPE;
int *numPEs;
fam *my_fam;
Fam_Options fam_opts;
Fam_Descriptor *itemLocal;
Fam_Region_Descriptor *descLocal;
enum APIS {GET,GET_NB,PUT, PUT_NB};
uint64_t fam_get_time() {
#if 1
        long int time = static_cast<long int>(
            duration_cast<nanoseconds>(
                high_resolution_clock::now().time_since_epoch())
                .count());
        return time;
#else // using intel tsc
        uint64_t hi, lo, aux;
        __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi), "=c"(aux));
        return (uint64_t)lo | ((uint64_t)hi << 32);
#endif
    }

uint64_t fam_time_diff_nanoseconds(uint64_t start,
                                       uint64_t end) {
        return (end - start);
    }

void print_fi_info() {

cout << "Libfabric fi_info" << endl;
cout << "--------------------------------------------" << endl;
system("fi_info -v -p cxi"); 
cout << "===============================================================" << endl;
cout << "Libfabric environment variables" << endl;
cout << "--------------------------------------------" << endl;
system("env | grep FI_");
cout << "===============================================================" << endl;
cout << "OpenFAM environment variables" << endl;
cout << "--------------------------------------------" << endl;
system("env | grep OPENFAM_");
cout << "--------------------------------------------" << endl;
 system("squeue -o '%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R %o' ");
cout << "===============================================================" << endl;
cout << "Loaded modules" << endl;
cout << "module load openfam" << endl;
cout << "===============================================================" << endl;
cout << "Slurm environment variables" << endl;
system("env | grep SLURM_");
cout << "--------------------------------------------" << endl;
cout << "===============================================================" << endl;
}

std::string size;
int main(int argc, char **argv)
{
    int c;
    uint64_t regionSize = BIG_REGION_SIZE;
    uint64_t dataitem_size = BIG_REGION_SIZE / 2;
    apis["get"] = GET;
    apis["get_nb"] = GET_NB;
    apis["put"] = PUT;
    apis["put_nb"] = PUT_NB;
    while ((c = getopt(argc, argv, "d:i:p:s:a:t:n:z")) != -1) {
	switch (c) {
/*                case 'i':
		id = atol(optarg);
		break;*/
/*                case 'k':clientkey=atol(optarg);
		 break;*/
		case 'i':
			num_cluster_nodes = atol(optarg);
			break;
		case 'p':
			ppn = atol(optarg);
			break;

		case 's':
			size = optarg;
			if (size.compare("all") == 0 )
				buf_sz = 0;
			else 
				buf_sz = atol(optarg);
			break;
		case 'd':
			device = strdup(optarg);
			break;
		case 'a':
			api_type = strdup(optarg);
			api_val = apis[api_type];
			break;
		case 't':
			wait_time = atol(optarg);
			break;
			//io_type = strdup(optarg);
			//cout << io_type << endl;
			break;
		case 'n':
			iters = atol(optarg);
			break;
		default:
			break;
	}
    }

    my_fam = new fam();

    init_fam_options(&fam_opts);
    fam_opts.if_device = (char *)strdup(device);
    char *local = (char *)malloc((size_t)dataitem_size);
    try {
    my_fam->fam_initialize("default", &fam_opts);
    } catch(Fam_Exception &e) {
        cout << "Init failed: " << e.fam_error_msg() << endl;
        exit(-1);
    }

    EXPECT_NO_THROW(myPE = (int *)my_fam->fam_get_option(strdup("PE_ID")));

    EXPECT_NE((void *)NULL, myPE);
    EXPECT_NO_THROW(numPEs = (int *)my_fam->fam_get_option(strdup("PE_COUNT")));
    EXPECT_NE((void *)NULL, numPEs);
    if (( gethostname(hostname,100)) < 0)  
	    cout << "hostname incorrect" << endl;
//    else
//	    cout << "hostname is " << hostname << endl;
	cout << "PE" << *myPE <<  " numpes: " << numPEs << endl;
    if ( *myPE == 0) {
	    print_fi_info();
	    for (int i = 1; i < argc; ++i) {
                printf("arg %2d = %s\n", i, (argv[i]));
            }

	    // Create Region and Allocate data item
	    Fam_Region_Attributes *regionAttributes  = (Fam_Region_Attributes *)calloc(1, sizeof(Fam_Region_Attributes));
  		try {
		descLocal =
			  my_fam->fam_create_region("testRegion", regionSize , 0777, regionAttributes);
		itemLocal = my_fam->fam_allocate(
					    "testDataItem", dataitem_size, 0777, descLocal);
		} catch(Fam_Exception &e) {
			cout << "Fam Exception: " << e.fam_error_msg() << endl;
		}
		EXPECT_NO_THROW(my_fam->fam_barrier_all());
		    uint64_t *keys = itemLocal->get_keys();
		    cout << "Using Keys: " << *keys << endl;

     		cout << "API,PEID,NODENAME,ITERS,SIZE,TOTAL_API_TIME(ns),AVERAGE_TIME(ns),AVERAGE_TIME_AFTER_BARRIER(ns),THROUGHPUT(GB/s),TOTALPES,PPN,NUM_CLUSTER_NODES,COREID,DEVICE" << endl;
    } else {
		EXPECT_NO_THROW(my_fam->fam_barrier_all());
		EXPECT_NO_THROW(descLocal = my_fam->fam_lookup_region("testRegion"));
		EXPECT_NE((void *)NULL, descLocal);
		itemLocal =  my_fam->fam_lookup("testDataItem", "testRegion");
		EXPECT_NE((void *)NULL, itemLocal);
     
    }
	EXPECT_NO_THROW(my_fam->fam_barrier_all());
        // Warmup function
     my_fam->fam_get_blocking(local, itemLocal, 0, (uint64_t)dataitem_size);
	
	EXPECT_NO_THROW(my_fam->fam_barrier_all());
    int count = 1;
    if ( buf_sz == 0 ) {
	count = sizeof(sizes) /sizeof(sizes[0]);
     }
     for ( int i = 0; i < count; i++ ) {
	if (count > 1)
	     	buf_sz = sizes[i];
	uint64_t size_per_pe = dataitem_size / *numPEs;
        uint64_t offset = *myPE * size_per_pe;
//  	cout << "PE" << *myPE <<  " starting offset: " << offset << "cxi device " << getenv("FI_CXI_DEVICE_NAME") <<endl;	     
        #ifdef TEST_PROFILE
        uint64_t total_api_time = 0;
        uint64_t total_api_time_with_barrier = 0;
        uint64_t profile_start = 0;
        uint64_t profile_end = 0;
        profile_start = fam_get_time();
        #endif
    // Do IOS based on api_type
	 for (int j = 0; j < iters; j++) {
		try {	
		 switch(api_val) {
		 case GET: 
			 my_fam->fam_get_blocking(local, itemLocal, offset, (uint64_t)buf_sz);				 
			 break;
		 case GET_NB: 
			 my_fam->fam_get_nonblocking(local, itemLocal, offset,(uint64_t) buf_sz);
			 my_fam->fam_quiet();
			 break;
		 case PUT: 
			 my_fam->fam_put_blocking(local, itemLocal, offset,(uint64_t) buf_sz);
			 break;
		 case PUT_NB: 
			 my_fam->fam_put_nonblocking(local, itemLocal, offset,(uint64_t) buf_sz);
			 my_fam->fam_quiet();
			 break;
		 default : break;
	         }
		 offset += buf_sz;
		 if ( offset >= ((*myPE + 1) * size_per_pe))
			 offset = (*myPE * size_per_pe);
		} catch(Fam_Exception &e) {
			cout << "api_val: " << api_val << " failed with exception " << e.fam_error_msg()  << " at iteration: " << j << endl;
		}
	}
/*		 switch(api_val) {
		 case GET_NB: 
		 case PUT_NB: 
			 try {
			 my_fam->fam_quiet();
			 } catch(Fam_Exception &e) {
				 cout << "fam_quiet exception " <<  e.fam_error_msg()  << endl;
			 }
		 } */
        #ifdef TEST_PROFILE
        profile_end = fam_get_time();
        total_api_time += fam_time_diff_nanoseconds(profile_start,profile_end);
        double avgtime_ns = (double)total_api_time/ (double)(iters);
        #endif
	EXPECT_NO_THROW(my_fam->fam_barrier_all());


        #ifdef TEST_PROFILE
        profile_end = fam_get_time();
        total_api_time_with_barrier += fam_time_diff_nanoseconds(profile_start,profile_end);
        double avgtime_ns_with_barrier = (double)total_api_time_with_barrier/ (double)(iters);
        cout << api_type << ",PE" << *myPE << "," << hostname << "," <<  iters << "," << (long)buf_sz << "," << total_api_time << "," << avgtime_ns << ","  << avgtime_ns_with_barrier << "," << (double)buf_sz/(double)avgtime_ns_with_barrier << "," << *numPEs << "," << ppn << "," << num_cluster_nodes << "," <<  sched_getcpu() << "," <<  getenv("FI_CXI_DEVICE_NAME") << endl;
        #endif
     }
	free(local);

    EXPECT_NO_THROW(my_fam->fam_barrier_all());

    if ( *myPE == 0) {
        //Deallocate data item and destroy region
	cout << "PE " << *myPE << "  doing deallocation and destroy region" << endl;
       	EXPECT_NO_THROW(my_fam->fam_deallocate(itemLocal));
        EXPECT_NO_THROW(my_fam->fam_destroy_region(descLocal));

    }    
    
    EXPECT_NO_THROW(my_fam->fam_finalize("default"));
    delete my_fam;

}
