// author: Eugene Ivanyuk (eugene.ivanyuk@gmail.com)
//
// dt - abbreviation for datetime


#define _XOPEN_SOURCE		// strptime() 
#include <stdio.h>
#include <stdlib.h>			// exit()
#include <fcntl.h>
#include <unistd.h>			// lseek(), read(), getopt()
//#include "dec_to_bin.c"	// dec_to_bin_ll()
//#include "../binbit.c"
#include <getopt.h>			// getopt_long()
#include "micro-bunzip.h"
#include <time.h>			// strptime(), tm structure
#include <stdbool.h>		// bool type
#include <errno.h>			// strerror()
#include <string.h>			// strstr()
#include <stdint.h>         // intmax_t
#include <ctype.h>          // isspace()

#define BUFFER_SIZE 8192
#define FIRST_BLK_POS 32

// debug switch
#define DEBUG 0

// macro for debug printing
#define debug_print(fmt, ...) \
        do { if (DEBUG) fprintf(stderr, "%d:%s(): " fmt "\n",\
                                __LINE__, __func__, __VA_ARGS__); } while (0)
// macro for errors printing
#define error_print(err_msg, ...) \
	fprintf(stderr, "\nLine %d, function %s(), ERROR:\n" err_msg "\n", \
				__LINE__, __func__, __VA_ARGS__);

// Supported datetime formats
const char * DATETIME_FORMATS[] = 
	{"%Y-%m-%dT%H:%M:%S",	/* "2017-02-21T14:53:22" */
     "%b %d %H:%M:%S",	    /* "Oct 30 05:54:01" */ 
     "%Y-%m-%d %H:%M:%S",	/* "2017-02-21 14:53:22" */
     "%d/%b/%Y:%H:%M:%S" }; /* "12/Dec/2015:18:39:27" */

// Functions declaration
void process_opts(int, char *[], const char **, const char **, const char **);
void usage(char *);
long long unsigned search_start_bit_of_bz2_blk(bunzip_data *);
// converts char string to epoch time (seconds since Jan 1 1970 00:00:00 UTC)
time_t convert_dt_str_to_epoch(const char *, const char *);
int uncompress_first_buf_of_blk(unsigned long, bunzip_data *, char *);
int uncompress_last_2_buffers_of_blk(unsigned long, bunzip_data *, char *);
const char * get_first_dt_str_from_bz2_blk(unsigned long, int, bunzip_data *,
                                            char *, const char *);
const char * get_last_dt_str_from_bz2_blk(unsigned long, int, bunzip_data *,
                                            char *, const char *);
bool is_dt_substr_in_str(char *, int, char *, int, const char *);
unsigned long opt_from_bin_search(off_t, off_t, time_t, bunzip_data *, 
                                const char *, int, char *, const char *);
bool is_dt_str_in_obuf(const char *, int , const char *);
int seek_dt_str_in_blk(unsigned long, bunzip_data *, const char *, bool *);
int uncompress_blk(unsigned long, bunzip_data *);
const char * def_dt_fmt(const char *);
unsigned long long opt_from_first_blk_search(unsigned long long, bunzip_data *,
                                             const char *);
long long find_last_blk_pos(bunzip_data *);
off_t lseek_set(bunzip_data *, off_t);
char * get_str(char *, int *, char *);


int main(int argc, char *argv[])
{
// Variables declaration:
    int ifd, status, dt_substr_len;
    off_t file_size;
    // options --from, --to, --file
    const char *opt_f, *opt_to, *opt_input_file;	
    bunzip_data *bd;
    // bit position of start of a block where opt_from/opt_to string was found
    off_t opt_from_pos, opt_to_pos;
    unsigned long long next_blk_pos;
    off_t cur_rel_bz2_blk_pos;
    // last bz2 block position in a file
    off_t last_blk_pos;
    // stores the time_t values of opt_f, opt_to strings
    time_t opt_from_time_t, opt_to_time_t, first_dt_str_in_outbuf_time_t;
    const char *opt_from_dt_fmt;
    const char *opt_to_dt_fmt;
    const char *dt_fmt;
    bool is_dt_str_found = false;
    // first/last dates in the input file
    const char *file_first_date, *file_last_date;	
    // first/last dates converted to time_t
    time_t file_first_date_time_t, file_last_date_time_t;		


// Process arguments
    process_opts(argc, argv, &opt_f, &opt_to, &opt_input_file);

// Open an input bz2 file
    if ((ifd = open(opt_input_file ,O_RDONLY)) < 0)
    {
	    //printf("main(), ERROR: open(opt_input_file ,O_RDONLY) < 0\n");
	    error_print("Can't open the file %s\n%s\n",
                    opt_input_file, strerror(errno));
	    exit(EXIT_FAILURE);
    }

// Check if the input file is in bzip2 format, prepare bd structure for work.
    if (status = start_bunzip(&bd, ifd, 0, 0))
    {
        error_print("start_bunzip() returned: %s\n", bunzip_errors[-status]);
	    exit(EXIT_FAILURE);
    }

// Define datetime formats of --from and --to arguments and check if they are
// equal.
    opt_from_dt_fmt = def_dt_fmt(opt_f);
    //printf("%s(): opt_f = \"%s\", datetime format = \"%s\"\n",
    //     __func__, opt_f, opt_from_dt_fmt);
    opt_to_dt_fmt = def_dt_fmt(opt_to);
    //printf("%s(): opt_to =   \"%s\", dt format = \"%s\"\n",
    //  __func__, opt_to, opt_to_dt_fmt);
    
    // Check if opt_f datetime format equals to opt_to datetime format
    if ( strcmp(opt_from_dt_fmt, opt_to_dt_fmt) != 0 ) 
    {
	    error_print("%s\n", "Values of --from and --to were not set in the same"
            " datetime format");
        exit(EXIT_FAILURE);
    }
    dt_fmt = opt_from_dt_fmt;
    //exit(EXIT_SUCCESS

// Convert opt_f, opt_to strings to epoch time to compare them
    opt_from_time_t = convert_dt_str_to_epoch(opt_f, dt_fmt);
    opt_to_time_t = convert_dt_str_to_epoch(opt_to, dt_fmt);

// Check if opt_f >= opt_to
    if ( opt_from_time_t >= opt_to_time_t )
    {
        error_print("%s\n", "Value of --from shouldn't be >= value of --to");
        exit(EXIT_FAILURE);
    }

    // Determine a length of a datetime substring.
    dt_substr_len = strlen(opt_f);
    //printf("dt_substr_len = %d\n", dt_substr_len);

    // first datetime substring which is started from a newline and was found in
    // an output buffer. +1 for null char.
    char first_dt_str_in_outbuf[dt_substr_len + 1];     
    // last datetime substring that was found in the last output buffer 
    char last_dt_str_in_outbuf[dt_substr_len + 1];

// Check if opt_f, opt_to are not outside the period, covered by an input file
    // get the first datetime substring (first_dt_str_in_outbuf) from the 
    // first block of a file

    file_first_date = get_first_dt_str_from_bz2_blk(FIRST_BLK_POS, dt_substr_len, 
                                                  bd, first_dt_str_in_outbuf, 
                                                  dt_fmt);
    
    debug_print("file_first_date (block %d) is: %s", 
                FIRST_BLK_POS, file_first_date);
    
    file_first_date_time_t = convert_dt_str_to_epoch(file_first_date, dt_fmt);
    //debug_print("file_first_date_time_t = %d", file_first_date_time_t);

    if (opt_from_time_t < file_first_date_time_t) 
    {
	    error_print("A value of --from shouldn't be < the first date in the file"
            	    " (%s)", file_first_date);
	    exit(EXIT_FAILURE);
    }

    // get the last datetime value from the last block of a file
    cur_rel_bz2_blk_pos = find_last_blk_pos(bd);
    //printf("%s: last block position = %llu\n", 
    //         __func__, cur_rel_bz2_blk_pos + bd->cur_file_offset * 8);
    //printf("%s: file_last_date (block %llu) is: %s\n", 
    //        __func__, cur_rel_bz2_blk_pos, file_first_date);
    file_last_date = get_last_dt_str_from_bz2_blk(cur_rel_bz2_blk_pos, 
        dt_substr_len, bd, last_dt_str_in_outbuf, dt_fmt);
    last_blk_pos = bd->cur_file_offset * 8 + cur_rel_bz2_blk_pos;
    debug_print("file_last_date (block %llu) is: %s\n",
                 last_blk_pos, file_last_date);
    file_last_date_time_t = convert_dt_str_to_epoch(file_last_date, dt_fmt);
    //printf("%s(): file_last_date_time_t = %d\n", __func__, file_last_date_time_t);

    if (opt_to_time_t > file_last_date_time_t)
    {
	    error_print("A value of --to shouldn't be > the last date in the file (%s)",
                    file_last_date);
        exit(EXIT_FAILURE);
    }

// Define a file size 
    file_size = lseek(bd->in_fd, 0, SEEK_END);
    
    //printf("%s(): lseek position BEFORE opt_from_bin_search = %lu\n",
    //        __func__, lseek(bd->in_fd, 0, SEEK_CUR ));

// Search a block where opt_f is located
    opt_from_pos = opt_from_bin_search(0, file_size, opt_from_time_t, bd, opt_f,
        dt_substr_len, first_dt_str_in_outbuf, dt_fmt);
                                       
    debug_print("opt_from_pos = %llu", opt_from_pos);
    debug_print("bd->cur_file_offset = %llu", bd->cur_file_offset);
    //debug_print("lseek position AFTER opt_from_bin_search = %lu", 
    //             lseek(bd->in_fd, 0, SEEK_CUR ));

    if (opt_from_pos + bd->cur_file_offset * 8 != FIRST_BLK_POS)
    {
        // Search for the very first block where opt_f is located
        opt_from_pos = opt_from_first_blk_search(opt_from_pos, bd, opt_f);
        //debug_print("The very first opt_from_pos = %llu", 
        //    bd->cur_file_offset * 8 + opt_from_pos);
        //printf("opt_from_pos + bd->cur_file_offset * 8 = %lu\n",
        //        opt_from_pos + bd->cur_file_offset * 8);
    }

// Save current relative position
    cur_rel_bz2_blk_pos = opt_from_pos;

    //exit(EXIT_SUCCESS);

// Set first_dt_str_in_outbuf_time_t of the first bz2 block, where opt_f was
// found, to opt_from_time_t 
    first_dt_str_in_outbuf_time_t = opt_from_time_t;

    //printf("main(): first_dt_str_in_outbuf_time_t = %llu, opt_to_time_t = %llu\n",
    //        first_dt_str_in_outbuf_time_t, opt_to_time_t);

// Uncompress the first block, where opt_f was found, and all the next blocks
// till the last one where first found datetime sting = opt_to
    while (first_dt_str_in_outbuf_time_t <= opt_to_time_t) {
        // Uncompress a block
#if !DEBUG
	    uncompress_blk(cur_rel_bz2_blk_pos, bd);
	    //debug_print("block %llu\n", bd->cur_file_offset * 8 + cur_rel_bz2_blk_pos);
#endif
        debug_print("block %llu\n", bd->cur_file_offset * 8 + cur_rel_bz2_blk_pos);

        // If an ucompressed block is the last one, then break a cycle
        if (bd->cur_file_offset * 8 + cur_rel_bz2_blk_pos == last_blk_pos)
            goto the_end;

        // Move file offset to the byte where current bz2 block was found + 1 
        // (i.e. next block)
	    bd->cur_file_offset = lseek_set(bd, bd->cur_file_offset + cur_rel_bz2_blk_pos / 8 + 1);
	    //printf("main(): lseek(bd->in_fd, bd->cur_file_offset + cur_rel_bz2_blk_pos / 8 + 1, SEEK_SET) = %llu\n", bd->cur_file_offset);

        // Search bit position of the next block
	    cur_rel_bz2_blk_pos = search_start_bit_of_bz2_blk(bd);
	    //printf("main(): next block position = %llu, bd->cur_file_offset = %llu\n",
        //        cur_rel_bz2_blk_pos, bd->cur_file_offset);

        // Uncompress the block and find the first dt sting there
	    get_first_dt_str_from_bz2_blk(cur_rel_bz2_blk_pos, dt_substr_len, bd,
                                      first_dt_str_in_outbuf, dt_fmt);
	    //printf("main(): first_dt_str_in_outbuf in the block %llu = %s\n", 
        //  bd->cur_file_offset * 8 + cur_rel_bz2_blk_pos, first_dt_str_in_outbuf);

        // Convert previously found diatetime string into epoch time format
	    first_dt_str_in_outbuf_time_t = convert_dt_str_to_epoch(first_dt_str_in_outbuf, dt_fmt);
	    //printf("main(): first_dt_str_in_outbuf_time_t = %d\n", first_dt_str_in_outbuf_time_t);
    }

    // Check if opt_to exists in a whole block
    status = seek_dt_str_in_blk(cur_rel_bz2_blk_pos, bd, opt_to, &is_dt_str_found);
    if (status) 
    {
	    error_print("seek_dt_str_in_blk() returned %s", bunzip_errors[-status]);
	    exit(EXIT_FAILURE);
    }

    //printf("%s(): is_dt_str_found = %d\n", __func__, is_dt_str_found);

// Find the last block where opt_to exists
    while (is_dt_str_found) 
    {
        debug_print("opt_to value %s was found in the block %llu", 
                    opt_to, cur_rel_bz2_blk_pos + bd->cur_file_offset * 8);
	    bd->cur_file_offset = lseek(bd->in_fd, bd->cur_file_offset + 
            cur_rel_bz2_blk_pos / 8 + 1, SEEK_SET);
	    cur_rel_bz2_blk_pos = search_start_bit_of_bz2_blk(bd);
	    seek_dt_str_in_blk(cur_rel_bz2_blk_pos, bd, opt_to, &is_dt_str_found);
    }

    //printf("main(): opt_to value was not found in the block %llu\n", cur_rel_bz2_blk_pos + bd->cur_file_offset * 8);

the_end:

    // Print a newline at the end
    printf("\n");

    return 0;
}


void process_opts(int argc, char *argv[], const char **opt_f,
                    const char **opt_to, const char **opt_input_file)
{
    int getopt_res;
    struct opt {
        bool is_set;    // was option set?
        char *name;     // option name
    };
    
    const struct option long_options[] = {
        {"from",   required_argument,  NULL,   'b'},
        {"to",     required_argument,  NULL,   'e'},
        {"file",   required_argument,  NULL,   'f'},
        {NULL,     0,                  NULL,   0  }
    };

    struct opt mandat_opts[] = { 
        {false, ""      },
        {false, "--from"},
        {false, "--to"  },
        {false, "--file"} 
    };

    // Parse the options and assign its values to variables
    while ( (getopt_res = getopt_long(argc, argv, "", long_options, NULL)) != -1 )
    {
        //printf("opt = %c\n", opt);
        switch (getopt_res)
	    {
            case 'b':
                *opt_f = optarg;
                mandat_opts[1].is_set = true;
                break;
            case 'e':
                *opt_to = optarg;
                mandat_opts[2].is_set = true;
		        break;
            case 'f':
                *opt_input_file = optarg;
                mandat_opts[3].is_set = true;
                break;
            default: /* '?' */
		        usage(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    for (int i = 1; i <= 3; i++)
    {
        if (mandat_opts[i].is_set == false) 
        {
            error_print("missing %s option", mandat_opts[i].name);
            usage(argv[0]);
            exit(EXIT_FAILURE);
        }
    }
    
}


long long find_last_blk_pos(bunzip_data *bd)
{
    long long last_blk_pos = 0;
    int backward_offset_step = 512;
    int backward_offset;

    // searching for the last bz2 block from file's end by backward_offset  
    for (backward_offset = backward_offset_step; last_blk_pos == 0; backward_offset += backward_offset_step)
    {
        bd->cur_file_offset = lseek(bd->in_fd, -backward_offset, SEEK_END);
        // search for the block
        last_blk_pos = search_start_bit_of_bz2_blk(bd);
        //printf("%s: last block position = %llu\n", __func__, last_blk_pos + bd->cur_file_offset * 8);
    }

    return last_blk_pos;
}


unsigned long long opt_from_first_blk_search(unsigned long long opt_from_pos, 
                                             bunzip_data *bd,
                                             const char * opt_f)
{
    long long prev_rel_bz2_blk_pos, cur_rel_bz2_blk_pos;
    long long prev_abs_bz2_blk_pos, cur_abs_bz2_blk_pos;
    long long backward_offset_step, abs_backward_offset_B;
    int status;
    bool is_dt_str_found = true;
    long prev_file_offset;


    backward_offset_step = 0;
    cur_rel_bz2_blk_pos = prev_rel_bz2_blk_pos = opt_from_pos;
    cur_abs_bz2_blk_pos = prev_abs_bz2_blk_pos = bd->cur_file_offset * 8 + opt_from_pos;

    //printf("opt_from_first_blk_search(): current file offset start of a function = %llu\n", lseek(bd->in_fd, 0, SEEK_CUR));

    //printf("opt_from_first_blk_search(lseek(INFO)): %llu B\n", lseek(bd->in_fd, 0, SEEK_SET));

// Search for the previous block
// Set file offset BUFFER_SIZE bytes backward
    while (is_dt_str_found == true)
    {
        // Preserve cur_file_offset for prev_rel_bz2_blk_pos
        prev_file_offset = bd->cur_file_offset;

	// Loop to find the previous block. 
        while (cur_abs_bz2_blk_pos == prev_abs_bz2_blk_pos)
        {
            if ((bd->cur_file_offset - BUFFER_SIZE) <= 0) 
            {
		        bd->cur_file_offset = lseek(bd->in_fd, 4, SEEK_SET);
	        }
	        else 
            {
                // Set file position to -BUFFER_SIZE byte
                bd->cur_file_offset = lseek(bd->in_fd, -BUFFER_SIZE, SEEK_CUR);
	        }

	        //printf("%s: lseek offset = %lld\n", __func__, bd->cur_file_offset);
            if (bd->cur_file_offset == -1) 
            {
                error_print("bd->cur_file_offset = lseek(bd->in_fd,"
                            " -BUFFER_SIZE, SEEK_CUR) = %lu",
                            bd->cur_file_offset);
                exit(EXIT_FAILURE);
            }
            //printf("opt_from_first_blk_search(): lseek(bd->in_fd, -%d, SEEK_CUR) = %llu\n", BUFFER_SIZE, bd->cur_file_offset);

        // Searching for the block starting from the abs_backward_offset_B byte
            cur_rel_bz2_blk_pos = search_start_bit_of_bz2_blk(bd);
	    // Convert found block position into absolute value
            cur_abs_bz2_blk_pos = bd->cur_file_offset * 8 + cur_rel_bz2_blk_pos;

	    //printf("search_start_bit_of_bz2_blk(): cur_rel_bz2_blk_pos %lli, prev_rel_bz2_blk_pos %lli\n\n", cur_rel_bz2_blk_pos, prev_rel_bz2_blk_pos);
	    //printf("opt_from_first_blk_search(): current file offset = %llu\n", lseek(bd->in_fd, 0, SEEK_CUR));
        //printf("opt_from_first_blk_search: cur_abs_bz2_blk_pos = %lli b, prev_abs_bz2_blk_pos = %lli b\n",\
			                                                  cur_abs_bz2_blk_pos, prev_abs_bz2_blk_pos);
	    //printf("%s: prev_file_offset = %lu\n", __func__, prev_file_offset);
        }

        // Check if opt_f exists in the current block
        status = seek_dt_str_in_blk(cur_rel_bz2_blk_pos, bd, opt_f, &is_dt_str_found);
        if (status)
        {
            error_print("seek_dt_str_in_blk(ERROR): %s", bunzip_errors[-status]);
            exit(EXIT_FAILURE);
        }
	    
        lseek_set(bd, bd->cur_file_offset);

        // If opt_f is found in current block then save it in prev_abs_bz2_blk_pos and search for the previous block
	    if (is_dt_str_found == true)
	    {
            prev_abs_bz2_blk_pos = cur_abs_bz2_blk_pos;
	        prev_rel_bz2_blk_pos = cur_rel_bz2_blk_pos;

	        // If current block is the first block (located in FIRST_BLOCK_POSnd bit) then stop searching for a previous block
	        debug_print("cur_abs_bz2_blk_pos = %lu", cur_abs_bz2_blk_pos);
	        if (cur_abs_bz2_blk_pos == FIRST_BLK_POS)
            {
                prev_file_offset = 4;
                break;
	        }
	    }
    }
    //printf("opt_from_first_blk_search: cur_rel_bz2_blk_pos = %lli b, prev_rel_bz2_blk_pos = %lli\n", cur_rel_bz2_blk_pos, prev_rel_bz2_blk_pos);
    //printf("opt_from_first_blk_search: cur_abs_bz2_blk_pos = %lli b, prev_abs_bz2_blk_pos = %lli\n", cur_abs_bz2_blk_pos, prev_abs_bz2_blk_pos);

    bd->cur_file_offset = lseek(bd->in_fd, prev_file_offset, SEEK_SET);
    //printf("opt_from_first_blk_search(INFO): return %lli cur_file_offset = %lli\n", prev_rel_bz2_blk_pos, bd->cur_file_offset);
    return prev_rel_bz2_blk_pos;
}


const char * def_dt_fmt(const char * dt_str)
{
    struct tm dt_tm = {0};
    short dt_fmts_arr_size;

    //printf("%s(): dt_str = %s\n", __func__, dt_str);

// Define a datetime format
    // 1. know an amount of supported dt formats (amount of elements
    // in DATETIME_FORMATS[])
    dt_fmts_arr_size = sizeof(DATETIME_FORMATS) / sizeof(DATETIME_FORMATS[0]);
    //printf("DATETIME_FORMATS size = %d\n", dt_fmts_arr_size);
    
    // 2. check if opt_f, opt_to strings correspond to one of supported
    // datetime formats
    for (int i = 0; i < dt_fmts_arr_size; i++)
    {
        if (strptime(dt_str, DATETIME_FORMATS[i], &dt_tm) != NULL)
            return DATETIME_FORMATS[i];
	}

// A given dt_sting doesn't correspond to one of supported dt formats. Error.
    error_print("%s", "The value of --from or --to arguments was set in "
		        "unsupported datetime format.\n"
	            "Supported datetime formats are:");
    for (short i = 0; i < dt_fmts_arr_size; i++)
        printf("\t%s\n", DATETIME_FORMATS[i]);
    printf("\n");
    exit(EXIT_FAILURE);
}


/* Function does bin search on the file's bytes level (lseek()). Then it searches
for a bz2 block which is nearest from the current middle byte element.
Then it finds the first datetime sting in the found bz2 block, converts it to 
epoch format and compares it to the opt_from_time_t value to understand where it
should continue searching the next middle byte. */
unsigned long opt_from_bin_search(off_t low,
                                  off_t high, 
                                  time_t opt_from_time_t,
                                  bunzip_data *bd,
                                  const char *opt_f,
                                  int dt_length,
                                  char *first_dt_str_in_outbuf,
                                  const char *dt_fmt)
{
    off_t mid, mid_pos;
    unsigned int mid_pos_in_bytes;
    time_t middle_time_t;
    time_t first_dt_str_in_outbuf_time_t;
    time_t last_dt_str_in_blk_time_t;
    char last_dt_str_in_blk[dt_length];


    while (low <= high)
    {
        mid = low + (high - low) / 2;
	    //printf("\n%s: low = %lu, mid = %lu, hig = %lu\n", 
        //        __func__, low, mid, high);
	    if (DEBUG) putchar('\n');
	    debug_print("low = %luB, mid = %luB, hig = %luB", low, mid, high);
        // Set file offset to mid byte
        bd->cur_file_offset = lseek_set(bd, mid);
/*        bd->cur_file_offset = lseek(bd->in_fd, mid, SEEK_SET);
        if (bd->cur_file_offset != mid){
            printf("%s(): ERROR. lseek(bd->in_fd, mid, SEEK_SET) returned %llu"
                   " which is not equal to mid (%llu)\n", 
                   __func__, bd->cur_file_offset, mid);
            exit(EXIT_FAILURE);
        }
*/
        //debug_print("lseek position = %lu", lseek(bd->in_fd, 0, SEEK_CUR ));
        
        mid_pos = search_start_bit_of_bz2_blk(bd);

	    debug_print("block %jd", (intmax_t)(bd->cur_file_offset * 8 + mid_pos));
        
        // Get the first dt string from current block and convert it to epoch time
	    get_first_dt_str_from_bz2_blk(mid_pos, dt_length, bd,
                                      first_dt_str_in_outbuf, dt_fmt);
        debug_print("first_dt_str_in_outbuf = %s", first_dt_str_in_outbuf);
        
        //debug_print("lseek position = %lu", lseek(bd->in_fd, 0, SEEK_CUR ));

	    first_dt_str_in_outbuf_time_t = convert_dt_str_to_epoch(first_dt_str_in_outbuf, dt_fmt);
	    //printf("opt_from_bin_search: first_dt_str_in_outbuf_time_t = %lu\n", 
        //  first_dt_str_in_outbuf_time_t);
	
        // Get the last dt string from the block and convert it to epoch time
        get_last_dt_str_from_bz2_blk(mid_pos, dt_length, bd, 
                                     last_dt_str_in_blk, dt_fmt);
	    debug_print("last_dt_str_in_blk = %s", last_dt_str_in_blk);

        // Set file offset back to the value which was before a call of the
        // function get_last_dt_str_from_bz2_blk.
        lseek_set(bd, bd->cur_file_offset);
        debug_print("lseek position = %lu", lseek(bd->in_fd, 0, SEEK_CUR ));

        last_dt_str_in_blk_time_t = convert_dt_str_to_epoch(last_dt_str_in_blk,
                                                            dt_fmt);
	    //printf("opt_from_bin_search: last_dt_str_in_blk_time_t = %lu\n", 
        //  last_dt_str_in_blk_time_t);

	    //printf("%s: opt_from_time_t = %lu\n", __func__, opt_from_time_t);
	    //printf("%s: opt_from_time_t(%lu) - first_dt_str_in_outbuf_time_t(%lu) = %lu\n", 
        //  __func__, opt_from_time_t, first_dt_str_in_outbuf_time_t, 
        //  opt_from_time_t - first_dt_str_in_outbuf_time_t);
	    if ( opt_from_time_t > first_dt_str_in_outbuf_time_t )
        {
            debug_print("opt_f (%s) > first_dt_str_in_outbuf (%s)", 
                        opt_f, first_dt_str_in_outbuf);

            if ( opt_from_time_t <= last_dt_str_in_blk_time_t )
            {
                // Break the loop and return the block position.
                debug_print("opt_f (%s) <= last_dt_str_in_blk (%s)", 
                            opt_f, last_dt_str_in_blk);
                //debug_print("lseek position = %lu", lseek(bd->in_fd, 0, SEEK_CUR));
                break;
            }

            debug_print("opt_f (%s) > last_dt_str_in_blk (%s)", 
                        opt_f, last_dt_str_in_blk);
	    
	        // Set low to the byte next to the current byte where current bz2 
            // block was found.
            // So we convert mid_pos from bits to bytes by / 8 and add 1
            //low = mid + mid_pos / 8 + 1;
            low = mid + 1;

	    } 
        else if (opt_from_time_t < first_dt_str_in_outbuf_time_t)
        {

            debug_print("opt_f (%s) < first_dt_str_in_outbuf (%s)\n", 
                        opt_f, first_dt_str_in_outbuf);

            // Set high to the byte previous to the current byte where current 
            // bz2 block was found.
            // So we convert mid_pos from bits to bytes by / 8 and subtract 1 
	        // high = mid + mid_pos / 8 - 1;
	        high = mid - 1;

	    }
        else
        {
            // If opt_f == first_dt_str_in_outbuf_time_t then break the loop and
            // return current block position
	        debug_print("opt_f (%s) == first_dt_str_in_outbuf (%s)", 
                    opt_f, first_dt_str_in_outbuf);

            //printf("opt_from_bin_search: opt_f was found in the block with starting bit %llu\n\n", mid_pos + bd->cur_file_offset * 8);
            // to uncompress the block in which opt_f was found it's needed to
            // move lseek to the previous position

            lseek_set(bd, bd->cur_file_offset);
 /*           lseek_result = lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET); 
            if (lseek_result != bd->cur_file_offset) {
                printf("\nERROR: line %d, function %s():\n"
		               "lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET) returned %llu,"
	       	           "which is not equal to bd->cur_file_offset(%llu)\n\n",
		                __LINE__, __func__, lseek_result, bd->cur_file_offset);
                exit(EXIT_FAILURE);
            }
*/
            break;
	    }
    }

    return mid_pos;
}

off_t lseek_set(bunzip_data *bd, off_t offset)
{
    off_t result = 0;

    result = lseek(bd->in_fd, offset, SEEK_SET);
    if (result != offset)
    {
        error_print("lseek(bd->in_fd (%d), offset (%jd), SEEK_SET) returned %jd,"
	       	        " which is not equal to offset",
                    bd->in_fd, (intmax_t)offset, (intmax_t)result);
        exit(EXIT_FAILURE);
    }

    return result;
}


/*
 * Seek the bunzip_data `bz` to a specific position in bits `pos` by lseeking
 * the underlying file descriptor and priming the buffer with appropriate
 * bits already consumed. This probably only makes sense for seeking to the
 * start of a compressed block.
 */
unsigned int seek_bits(bunzip_data *bd, unsigned long pos)
{
    long n_byte = pos / 8;
    char n_bit = pos % 8;
    
    debug_print("pos = %lu, n_byte = %d, n_bit = %d", pos, n_byte, n_bit);
    debug_print("lseek position = %lu, n_byte(%lu) + bd->cur_file_offset(%lu)", 
        lseek(bd->in_fd, 0, SEEK_CUR), n_byte, bd->cur_file_offset);
 
    // Seek the underlying file descriptor + bd->cur_file_offset because lseek
    // returns the amount of bytes from the beginning of a file
    if ( (lseek( bd->in_fd, n_byte, SEEK_CUR)) != n_byte + bd->cur_file_offset) 
    {
    //if ( (lseek( bd->in_fd, n_byte, SEEK_CUR)) != n_byte) {
        debug_print("current file offset AFTER lseek = %ld", 
                    lseek(bd->in_fd, 0, SEEK_CUR));
        
	    error_print("lseek( bd->in_fd, n_byte(%lu), SEEK_CUR)) != "
                    "n_byte(%lu) + bd->cur_file_offset(%lu)",
                    n_byte, n_byte, bd->cur_file_offset);
        exit(EXIT_FAILURE);
    }

    bd->inbufBitCount = bd->inbufPos = bd->inbufCount = 0;

    get_bits(bd, n_bit);

    return pos;
}


int uncompress_first_buf_of_blk(unsigned long pos, bunzip_data *bd, char *obuf)
{
    int status;
    int gotcount = 0;


    seek_bits( bd, pos );

    /* Fill the decode buffer for the block */
    if (status = get_next_block( bd ))
    {
        error_print("get_next_block() returned %d, bunzip_errors[-status]",
                     status);
        exit(EXIT_FAILURE);
    }

    /* Init the CRC for writing */
    bd->writeCRC = 0xffffffffUL;
 
    /* Zero this so the current byte from before the seek is not written */
    bd->writeCopies = 0;

    /* Decompress the first buffer of block */
    gotcount = read_bunzip(bd, obuf, BUFFER_SIZE);

    bd->cur_file_offset = lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET);

    if (gotcount < 0)
    {
        status = gotcount;
	    error_print("read_bunzip() returned %d, %s",
            gotcount, bunzip_errors[-status]);
	    exit(EXIT_FAILURE);
    }
    else if (gotcount == 0)
    {
	    status = gotcount;
        error_print("read_bunzip() returned %d, %s", 
            gotcount, bunzip_errors[-status]);
        exit(EXIT_FAILURE);
    }

    return gotcount;
}


int uncompress_last_2_buffers_of_blk(unsigned long pos, bunzip_data *bd, char *two_last_outbufs)
{
    int status;
    int gotcount, prev_gotcount, last_gotcount, totalcount;
    char prev_outbuf[BUFFER_SIZE + 1], last_outbuf[BUFFER_SIZE + 1];


    gotcount = prev_gotcount = last_gotcount = totalcount = 0;
    // Fill prev_outbuf and last_outbuf with 0s
    memset(prev_outbuf, 0, BUFFER_SIZE); 
    memset(last_outbuf, 0, BUFFER_SIZE); 
    prev_outbuf[BUFFER_SIZE] = '\0';
    last_outbuf[BUFFER_SIZE] = '\0';

    seek_bits( bd, pos );

    /* Fill the decode buffer for the block */
    if ((status = get_next_block( bd )))
    {
        printf("%s: ERROR. get_next_block() returned %d, bunzip_errors[-status]\n", __func__, status);
        exit(EXIT_FAILURE);
    }

    /* Init the CRC for writing */
    bd->writeCRC = 0xffffffffUL;

    /* Zero this so the current byte from before the seek is not written */
    bd->writeCopies = 0;

    for ( ;; )
    {
        /* Decompress the first buffer of block */
        gotcount = read_bunzip(bd, last_outbuf, BUFFER_SIZE);

    	// Sum up all gotcounts
    	totalcount += gotcount;

        //lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET);

        if ( gotcount < 0 ) 
        {
            status = gotcount;
            error_print("read_bunzip() returned %d, %s", 
                gotcount, bunzip_errors[-status]);
            exit(EXIT_FAILURE);
        }
	    else if (gotcount == BUFFER_SIZE) 
        {
            // If gotcount == BUFFER_SIZE then this is not the last buffer. So store it into prev_outbuf
	        memcpy (prev_outbuf, last_outbuf, BUFFER_SIZE);
	        prev_gotcount = gotcount;
	    }
	    else if (gotcount > 0 && gotcount < BUFFER_SIZE) 
        {
            // if obuf was written not in full then this is the last buffer.
	        // So store the content of prev_outbuf and last_outbuf into two_last_oubufs
	        last_gotcount = gotcount;
	        memcpy(&two_last_outbufs[0], prev_outbuf, sizeof(char) * prev_gotcount);
	        memcpy(&two_last_outbufs[BUFFER_SIZE], last_outbuf, sizeof(char) * last_gotcount);
            // Put null char after data end
	        two_last_outbufs[prev_gotcount + last_gotcount] = '\0';
            break;
	    }
	    else if (gotcount == 0 && totalcount == 0)
        {
/*        else if ( gotcount == 0 ) { // if gotcount == 0 obuf is not rewritten, so it contains previous buffer data
            if ( prev_gotcount > 0 ) {// if prev_gotcount is not empty this obuf is the last one in this block
                break;
            } */
	        // If the first read_bunzip() wrote 0 bytes into last_outbuf, exit with error.
	        status = gotcount;
	        debug_print("read_bunzip() returned %d uncompressing the block %llu. %s",
                gotcount, bd->cur_file_offset * 8 + pos, bunzip_errors[-status]);
            exit(EXIT_FAILURE);
        }
    }

    //printf("%s: obuf = %s\n", __func__, obuf);
    return prev_gotcount + last_gotcount;
}


int seek_dt_str_in_blk(unsigned long pos, bunzip_data *bd, const char * opt_f,
    bool * is_dt_str_found)
{
    int status = 0, i = 0;
    int gotcount = 0;
    char obuf[BUFFER_SIZE];
    int seek_bits_return;


    //printf("seek_dt_str_in_blk: &bd = %p, bd = %p\n", &bd, bd);
    
    seek_bits_return = seek_bits( bd, pos );
    //printf("seek_dt_str_in_blk: seek_bits returned %d\n", seek_bits_return);
    //printf("seek_dt_str_in_blk: opt_f = %s\n", opt_f);

    /* Fill the decode buffer for the block */
    if ( ( status = get_next_block( bd ) ) )
        goto seek_bunzip_finish;
    /* Init the CRC for writing */
    bd->writeCRC = 0xffffffffUL;

    /* Zero this so the current byte from before the seek is not written */
    bd->writeCopies = 0;

    /* Decompress the block and write to stdout */
    for ( ; ; i++ )
    {
        gotcount = read_bunzip( bd, obuf, BUFFER_SIZE );
    //printf("seek_dt_str_in_blk: read_bunzip returned %d\n", gotcount);
        if ( gotcount < 0 )
        {
            status = gotcount;
            break;
        }
        else if ( gotcount == 0 )
        {
            break;
        }
        else
        {
            // Here we have uncrompressed data in obuf

            //printf("seek_dt_str_in_blk: iteration %d\n", i);
            // Search from/to strings in obuf. As soon as stings was found - stop checking further blocks.
            if (*is_dt_str_found = is_dt_str_in_obuf(opt_f, gotcount, obuf))
            {
		        //printf("seek_dt_str_in_blk: is_dt_str_found = %d\n", is_dt_str_found);
		        goto seek_bunzip_finish;
		    }
            //printf("seek_dt_str_in_blk: iteration %d, gotcount = %d\n", i, gotcount);


                //write( 1, obuf, gotcount );
                //exit(EXIT_SUCCESS);
        }
    }

seek_bunzip_finish:

//    if ( bd->dbuf ) free( bd->dbuf );
//    free( bd );
    return status;
}


// Function gets the first datetime string which corresponds to one of known 
// datetime formats
const char* 
get_first_dt_str_from_bz2_blk(  unsigned long   pos, 
                                int             test_substr_len,
                                bunzip_data*    bd,
                                char*           first_dt_str_in_outbuf,
                                const char*     dt_fmt  )
{
    // byte/char position within obuf
    int obuf_pos;
    // sting with the same size as opt_f/opt_to to check if it matches to dt_fmt
    // + 1 for null char
    char test_substr [test_substr_len + 1];
    // first obuf from which first dt string should be searched
    char obuf[BUFFER_SIZE + 1];
    // amount of uncompressed bytes which was read to obuf
    int gotcount;
    // string length
    int str_len;

    
    //debug_print("pos = %u, bd->cur_file_offset = %lu, abs_pos = %lu\n", 
    //    pos, bd->cur_file_offset, bd->cur_file_offset * 8 + pos);

// Clean first_dt_str_in_outbuf from the previous value
    memset(first_dt_str_in_outbuf, 0, test_substr_len);

// Get the first uncompressed obuf from a block
    gotcount = uncompress_first_buf_of_blk(pos, bd, obuf);

// Finish obuf with '\0' char
    obuf[gotcount] = '\0';

// A string from obuf. We don't know beforehand its size, so set it a max
// possible size (gotcount bytes) to prevent buffer overflow. +1 for null char
    char str[gotcount + 1];

// As a buffer usually starts not from the beginning of a string but from
// arbitrary char, search for the first newline char (beginning of the next string)
    for (obuf_pos = 0; obuf_pos < gotcount; obuf_pos++) 
    {
        //printf("obuf[%d] = %c\n", obuf_pos, obuf[obuf_pos]);
        if (obuf[obuf_pos] == '\n')
            break;
    }

// Loop through obuf
    while (obuf_pos < gotcount) 
    {
        // Get a string from obuf
        get_str(obuf, &obuf_pos, str);
        debug_print("str = \"%s\"", str);
        
        //debug_print("obuf[%d] = '%c'", obuf_pos, obuf[obuf_pos]);
        
        str_len = strlen(str);
        //debug_print("str_len = %d", str_len);
        if (str_len <= test_substr_len)
            continue;

        bool status = is_dt_substr_in_str(str, str_len, test_substr,
            test_substr_len, dt_fmt);
        if (status)
        {
            // Copy test_substr to first_dt_str_in_outbuf and return it.
            strncpy(first_dt_str_in_outbuf, test_substr, test_substr_len + 1);
            break;
        }
    }

    return first_dt_str_in_outbuf;
}


// Function searches for the last datetime substring in the end of the last bz2
// block
const char* 
get_last_dt_str_from_bz2_blk(   unsigned long   pos,
                                int             test_substr_len, 
                                bunzip_data*    bd,
                                char*           last_dt_str_in_outbuf,
                                const char*     dt_fmt  )
{
    // byte/char position within obuf
    int obuf_pos;
    int last_char_pos;
    // sting with the same size as opt_f/opt_to to check if it matches to dt_fmt
    // + 1 for null char
    char test_substr [test_substr_len + 1]; 
    // 2 last output buffers from which the last datetime string should be
    // searched for
    char obuf2 [BUFFER_SIZE * 2 + 1]; 	
    // amount of uncompressed bytes which was read to obuf
    int gotcount;
    // Current string length in obuf2
    int str_len;
    // Previous position of a newline char in obuf2
    int prev_nl_pos;


// Clean last_dt_str_in_outbuf from the previous value
    memset(last_dt_str_in_outbuf, 0, test_substr_len);

// Clean obuf2 from the previous value
    memset(obuf2, 0, BUFFER_SIZE * 2);

// Get 2 last uncompressed output buffers from a block. Why 2? Becuase the last
// buffer usually isn't full and it's possible that it doesn't contain the dt
// string
    gotcount = uncompress_last_2_buffers_of_blk(pos, bd, obuf2);
    //printf("%s(): gotcount = %d\n", __func__, gotcount);

// Finish obuf2 with '\0' char
    obuf2[gotcount] = '\0';

    last_char_pos = gotcount - 1; //Why -1? Because if gotcount = 100 then an
    // index of the last element in an obuf2 array is 100 - 1 = 99
    prev_nl_pos = last_char_pos;

// A string from obuf2. We don't know beforehand its size, so set it a max
// possible size (gotcount bytes) to prevent buffer overflow 
    char str[gotcount + 1];

// Loop through obuf2 in reverse direction
    for (obuf_pos = last_char_pos; obuf_pos >= 0; obuf_pos--)
    {
	    //printf("%c", obuf2[obuf_pos]);
        
	    if (obuf2[obuf_pos] != '\n')
	        continue;

	    prev_nl_pos = obuf_pos;

        // Get a string from obuf
        get_str(obuf2, &obuf_pos, str);
        debug_print("str = \"%s\"", str);

        // If string length is less or equal to datetime substring length then
        // go to the previous string
        str_len = strlen(str);
        if (str_len <= test_substr_len)
        {   debug_print("str_len = %d\n", str_len);
            // go to the previous newline
            obuf_pos = prev_nl_pos; 
            continue;
        }

        bool status = is_dt_substr_in_str(str, str_len, test_substr,
                            test_substr_len, dt_fmt);
        if (status)
        {
            // Copy test_substr to first_dt_str_in_outbuf and return it.
            strncpy(last_dt_str_in_outbuf, test_substr, test_substr_len + 1);
            break;
        }
        else
        {
            // go to the previous newline
            obuf_pos = prev_nl_pos;
            continue;
        }
    }

    return last_dt_str_in_outbuf;
}


bool is_dt_str_in_obuf(const char * dt_str, int gotcount, const char * obuf)
{
    int obuf_pos, dt_byte_pos;
    int len_dt_str = strlen(dt_str);
    char found_dt_str[len_dt_str];

//    printf("is_dt_str_in_obuf: dt_str = %s\n\n", dt_str);

    dt_byte_pos = 0;

    // Loop to check every character from obuf to find dt_sting(s)
    for (obuf_pos = 0; obuf_pos < gotcount; obuf_pos++) 
    {
        // Search the substring in a buffer that matches to from argument
        if (obuf[obuf_pos] == dt_str[dt_byte_pos]) 
        {
            //if (obuf_pos == 0)
    	    found_dt_str[dt_byte_pos] = dt_str[dt_byte_pos];
	        //printf("found_dt_str[%d] = %c\n", dt_byte_pos, found_dt_str[dt_byte_pos]);

            // If the first character was matched and this is the first character
            // of a line - increase dt_byte_pos index to check if the next 
            // characters are matched on the next iteration the statement 
            // '&& obuf_pos != 0' protects from going before the obuf
            if (obuf[obuf_pos - 1] == '\n' && obuf_pos > 0) 
            {
                //printf("dt_str[%d] = obuf[%d] = %c\n",
                //        dt_byte_pos, obuf_pos, obuf[obuf_pos]);
                dt_byte_pos = 1;
                continue;	// go to the next cycle
            }

            // if the first character in a line was already matched continue to
            // check if the rest are also matched
            if (dt_byte_pos >= 1 && dt_byte_pos <= len_dt_str ) 
            {
                //printf("is_dt_str_in_obuf: dt_str[%d] = obuf[%d] = %c\n",
                //        dt_byte_pos, obuf_pos, obuf[obuf_pos]);
                dt_byte_pos++;

                if (dt_byte_pos == len_dt_str)
		        {
                    //printf("is_dt_str_in_obuf: match! dt_str = %s\n", dt_str);
                    return true;
                }
	       
                continue;	// go to the next cycle
            }
        }
        else 
        {
            dt_byte_pos = 0;
        }

        //printf("dt_byte_pos = %d, obuf[%d] = %c\n",
        //        dt_byte_pos, obuf_pos, obuf[obuf_pos]);
    }

    return false;
}


int uncompress_blk(unsigned long pos, bunzip_data *bd)
{
    int status = 0, i = 0;
    int gotcount = 0;
    char obuf[BUFFER_SIZE];


    seek_bits( bd, pos );
    
    /* Fill the decode buffer for the block */
    if (( status = get_next_block( bd ) ))
        goto seek_bunzip_finish;

    /* Init the CRC for writing */
    bd->writeCRC = 0xffffffffUL;

    /* Zero this so the current byte from before the seek is not written */
    bd->writeCopies = 0;

    /* Decompress the block and write to stdout */
    for ( ; ; i++ )
    {
        gotcount = read_bunzip( bd, obuf, BUFFER_SIZE );
        if ( gotcount < 0 )
        {
            status = gotcount;
            break;
        }
        else if ( gotcount == 0 )
        {
            break;
        }
        else
        {
            // Here we have uncrompressed data in obuf
            write( 1, obuf, gotcount );
        }
    }

seek_bunzip_finish:

//    if ( bd->dbuf ) free( bd->dbuf );
//    free( bd );
    if ( status ) fprintf( stderr, "\n%s\n", bunzip_errors[-status] );

    return status;
}


/* Converts char datetime string to epoch time (seconds since 
   Jan 1 1970 00:00:00 UTC) */
time_t convert_dt_str_to_epoch(const char * dt_str, const char * dt_fmt)
{
    // dt data structures to which opt_f, opt_to should be converted
    struct tm dt_tm = {0}; 
    time_t dt_time_t;

    // Convert string to tm structure format (tm structure from strings.h)
    // struct tm {
    //         int tm_sec;    /* Seconds (0-60) */
    //         int tm_min;    /* Minutes (0-59) */
    //         int tm_hour;   /* Hours (0-23) */
    //         int tm_mday;   /* Day of the month (1-31) */
    //         int tm_mon;    /* Month (0-11) */
    //         int tm_year;   /* Year - 1900 */
    //         int tm_wday;   /* Day of the week (0-6, Sunday = 0) */
    //         int tm_yday;   /* Day in the year (0-365, 1 Jan = 0) */
    //         int tm_isdst;  /* Daylight saving time */
    // };

    debug_print("dt_fmt = %s, dt_str = %s\n", dt_fmt, dt_str);
    if ( NULL == strptime(dt_str, dt_fmt, &dt_tm) )
    {
        error_print("strptime(dt_str, dt_fmt, &dt_tm) "
		    "can't convert received datetime string \"%s\" into tm "
		    "structure properly", dt_str);
	    exit(EXIT_FAILURE);
    }

//    printf("%s(): dt_tm.tm_sec (Seconds (0-60)) =\t%d\n", __func__, dt_tm.tm_sec);
//    printf("%s(): dt_tm.tm_min (Minutes (0-59)) =\t%d\n", __func__, dt_tm.tm_min);
//    printf("%s(): dt_tm.tm_hour (Hours (0-23)) =\t%d\n", __func__, dt_tm.tm_hour);
//    printf("%s(): dt_tm.tm_mday (Day of the month (1-31)) =\t%d\n", __func__, dt_tm.tm_mday);
//    printf("%s(): dt_tm.tm_mon (Month (0-11)) =\t%d\n", __func__, dt_tm.tm_mon);
//    printf("%s(): dt_tm.tm_year (Year - 1900) =\t%d\n", __func__, dt_tm.tm_year);

    // convert tm to time_t
    if ( (dt_time_t = mktime(&dt_tm)) == -1 ) {
        printf("mktime() returned -1. The specified broken-down dt(dt_time_t) cannot be "
               "represented as calendar time a(seconds since Epoch)\n");
        exit(EXIT_FAILURE);
    }
    //printf("convert_dt_str_to_epoch: dt_time_t = %d\n", dt_time_t);
    

    return dt_time_t;
}


// Function searches for a bit number of the nearest bz2 block starting from
// current lseek position
unsigned long long search_start_bit_of_bz2_blk(bunzip_data *bd)
{
    // amount of bytes which was read to inbuf during one 'read' operation
    int inbuf_read;
    // amount of bytes which was read from inbuf in total
    unsigned long long inbuf_read_total;
    // input buffer where data is read from a file
    unsigned char inbuf[BUFFER_SIZE];
    // counter for bytes which is reading from ibuf
    unsigned long long inbuf_byte_pos;
    // 64 bits which are read from an input buffer. We search for a needle in
    // this hay
    unsigned long long hay;	
    // all possible positions of bits of a needle within a hay
    unsigned long long needle_shifted[9];
    // all needed variants of masks. A mask ANDs with a hay
    unsigned long long masks[9];
    // magic sequence a new block is started from (BCD (pi))
    unsigned long long const needle = 0x314159265359;
   							// and compares with a needle
    // byte number a needle was found in
    unsigned long long position;

    inbuf_read_total = 0;
    hay = 0;
    position = 0;

    for (int i = 0; i <= 8; i++) {
        needle_shifted[i] = needle << i;
	    masks[i] = 0xffffffffffff << i;
    }

/* 	needle_shifted[0] =     0000000000000000001100010100000101011001001001100101001101011001
	masks[0] =              0000000000000000111111111111111111111111111111111111111111111111

	needle_shifted[1] =     0000000000000000011000101000001010110010010011001010011010110010
	masks[1] =              0000000000000001111111111111111111111111111111111111111111111110

	needle_shifted[2] =     0000000000000000110001010000010101100100100110010100110101100100
	masks[2] =              0000000000000011111111111111111111111111111111111111111111111100

	needle_shifted[3] =     0000000000000001100010100000101011001001001100101001101011001000
	masks[3] =              0000000000000111111111111111111111111111111111111111111111111000

	needle_shifted[4] =     0000000000000011000101000001010110010010011001010011010110010000
	masks[4] =              0000000000001111111111111111111111111111111111111111111111110000

	needle_shifted[5] =     0000000000000110001010000010101100100100110010100110101100100000
	masks[5] =              0000000000011111111111111111111111111111111111111111111111100000

	needle_shifted[6] =     0000000000001100010100000101011001001001100101001101011001000000
	masks[6] =              0000000000111111111111111111111111111111111111111111111111000000

	needle_shifted[7] =     0000000000011000101000001010110010010011001010011010110010000000
	masks[7] =              0000000001111111111111111111111111111111111111111111111110000000

	needle_shifted[8] =     0000000000110001010000010101100100100110010100110101100100000000
	masks[8] =              0000000011111111111111111111111111111111111111111111111100000000 */

/*    for (i = 0; i <= 8; i++)
    {
	printf("needle_shifted[%d] = \t%s\n", i, dec_to_bin_ll(needle_shifted[i]));
	printf("masks[%d] = \t\t%s\n", i, dec_to_bin_ll(masks[i]));
	putchar('\n');    
    } */

    //printf("search_start_bit_of_bz2_blk: lseek position BEFORE read() = %lu\n",
    //       lseek(bd->in_fd, 0, SEEK_CUR));

    // Search a needle starting from every next bit. Read BUFFER_SIZE bytes from
    // an input file to an inbuf buffer.
    while ((inbuf_read = read(bd->in_fd, inbuf, BUFFER_SIZE)) > 0)
    {
    //printf("search_start_bit_of_bz2_blk: lseek position AFTER read() = %lu\n",
    //         lseek(bd->in_fd, 0, SEEK_CUR));
	//printf("inbuf_read = %d\n", inbuf_read);
	
	// Take every byte from inbuf and put it into a hay. During every iteration
    // the most right byte of a hay is shifted to the left and a new byte from
    // inbuf is put to the hay. Inside hay shift every byte from right to left
    // and is compared to every needle_shifted.
	    for (inbuf_byte_pos = 0; inbuf_byte_pos < inbuf_read; inbuf_byte_pos++)
	    {
//	    printf("search_start_bit_of_bz2_blk: inbuf_read_total = %d\n", inbuf_read_total);
//          printf("hay = (hay << 8) | inbuf[%d] = \t", inbuf_byte_pos);
            
            // Shift the content of hay one byte to the left and put a new byte from inbuf to the most right position
	        hay = (hay << 8) | inbuf[inbuf_byte_pos];
	        //printf("%40s\n", dec_to_bin_ll(hay));
	        if (inbuf_byte_pos >= 6)
	        {
                for (int i = 0; i <= 8; i++)
                {
//		    printf("hay = \t\t\t\t%s\n", dec_to_bin_ll(hay));
    //		    printf("masks[%d] = \t\t\t%s\n", i, dec_to_bin_ll(masks[i]));
    //		    printf("hay & masks[%d] = \t\t%s\n", i, dec_to_bin_ll(hay & masks[i]));
    //		    printf("needle_shifted[%d] = \t\t%s\n", i, dec_to_bin_ll(needle_shifted[i]));

	            if ( (hay & masks[i]) == needle_shifted[i] ) // if needle was found, define correct position
		        {
		            if (i == 0) 
                    {
		                position = (inbuf_read_total + inbuf_byte_pos + 1 - 6) * 8;
//			    printf("search_start_bit_of_bz2_blk: inbuf_read_total = %llu, inbuf_byte_pos = %llu\n", inbuf_read_total, inbuf_byte_pos);
		            }
		            else if (i > 0 && i <= 8) 
                    {
			            position = (inbuf_read_total + inbuf_byte_pos + 1 - 7) * 8 + 8 - i;
			    //printf("search_start_bit_of_bz2_blk: position = (inbuf_read_total(%llu) + inbuf_byte_pos(%llu) + 1 - 7) * 8 + 8 - i(%d) = %lld\n", inbuf_read_total, inbuf_byte_pos, i, position);
		            }
		            else 
                    {
			            position = (inbuf_read_total + inbuf_byte_pos + 1 - 8) * 8 + 8 - i;
//			    printf("search_start_bit_of_bz2_blk: inbuf_read_total = %llu, inbuf_byte_pos = %llu\n", inbuf_read_total, inbuf_byte_pos);
		            }

		        //printf("search_start_bit_of_bz2_blk: position = %llu\n", position);
		        hay = 0; // to prevent from finding duplicates
                        //printf("search_start_bit_of_bz2_blk: lseek position BEFORE lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET) = %lu, bd->cur_file_offset = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR), bd->cur_file_offset);
                    // set file offset back to the value which was before call
                    // of this function. Because the found position is correct
                    // relatively to that file offset.
                    lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET);
		        //break;
                        //printf("search_start_bit_of_bz2_blk: lseek position BEFORE lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET) = %lu, bd->cur_file_offset = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR), bd->cur_file_offset);
                        //printf("search_start_bit_of_bz2_blk: lseek position AFTER lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET) = %lu, bd->cur_file_offset = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR), bd->cur_file_offset);
		        //printf("search_start_bit_of_bz2_blk: position = %llu bits\n\n", position);
		        return position;
		    }
	        }
	    }
//	    putchar('\n');
	}
	inbuf_read_total += inbuf_read;
        
    }
}

// Get a string from a buf strarting from buf_pos
char * get_str(char *buf, int *buf_pos, char *str)
{
    int str_pos = 0;


    // Current char is the newline char. So go to the first char of a string.
    // (*buf_pos)   first dereference buf_pos pointer to get obuf_pos value
    // ++           and then increment that value 
    (*buf_pos)++;

    //debug_print("buf[%d] = '%c'\n", *buf_pos, buf[*buf_pos]);
    while ( buf[*buf_pos] != '\n' && buf[*buf_pos] != '\0' )
    {
        str[str_pos++] = buf[(*buf_pos)++];
        //debug_print("buf[%d] = '%c'", *buf_pos, buf[*buf_pos]);
    }
    //debug_print("str_pos = %d", str_pos);
    
    // Finish str with a null char
    str[str_pos] = '\0';

    return str;
}


bool is_dt_substr_in_str(  char * str, int str_len, char * test_substr,
                        int test_substr_len, const char * dt_fmt)
{
    // Char position in a string
    int str_pos;
    // Char position in test datetime substring
    int test_substr_pos;
    // dt data structures opt_f, opt_to should be converted to
    struct tm dt_tm = {0};

    // Loop through a string
    for (str_pos = 0; str_pos < str_len; str_pos++)
    {
        // Take test_substr_len chars from a string into test_substr[]. 
        for (test_substr_pos = 0; test_substr_pos < test_substr_len; test_substr_pos++)
        {
            if (test_substr[test_substr_pos] == '\n')
                return false;

            // copy a char from a string to test_substr
            test_substr[test_substr_pos] = str[str_pos++];
        }

        // Add null char to the end of test_substr to make it C string
        test_substr[test_substr_len] = '\0';
    
        if (strlen(test_substr) < test_substr_len) return false;

        //debug_print("obuf_pos = %d", obuf_pos);
        //debug_print("test_substr = \"%s\"", test_substr);

        // If test_substr doesn't match to the dt_fmt (strptime == NULL)
        if ( strptime(test_substr, dt_fmt, &dt_tm) == NULL )
        {
            // Move str_pos test_substr_len chars back (i.e. to the char which
            // is new start char. It is next char to the previous start char)
            str_pos = str_pos - test_substr_len;
            continue;
        }
        else
        {
            //printf("%s: first_dt_str_in_outbuf = %s\n", __func__, first_dt_str_in_outbuf);
            return true;
        }

    }

    return false;
}

void usage(char * program_name)
{
    printf("Usage: %s --from=\"date\" --to=\"date\" --file=/path/to/file.bz2\n", 
            program_name);
}
