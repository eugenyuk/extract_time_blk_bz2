// author: Eugene Ivanyuk (eugene.ivanyuk@gmail.com)
//
// Legend:
// dt - abbreviation for datetime


#define _XOPEN_SOURCE			// 
#include <stdio.h>
#include <stdlib.h>			// exit()
#include <fcntl.h>
#include <unistd.h>			// lseek(), read(), getopt()
//#include "dec_to_bin.c"			// dec_to_bin_ll()
//#include "../binbit.c"
#include <getopt.h>			// getopt_long()
#include "micro-bunzip.h"
#include <time.h>			// strptime(), tm structure
#include <stdbool.h>			// bool type
#include <errno.h>			// strerror()
#include <string.h>			// strstr()

#define BUFFER_SIZE 8192
#define FIRST_BLOCK_POS 32

// debug switch
#define DEBUG 1

// macro for debug printing
#define debug_print(fmt, ...) \
        do { if (DEBUG) fprintf(stderr, "%d:%s(): " fmt "\n",\
                                __LINE__, __func__, __VA_ARGS__); } while (0)
// macro for errors printing
#define error_print(err_msg, ...) \
	fprintf(stderr, "\nLine %d, function %s(), ERROR:\n" err_msg "\n", \
				__LINE__, __func__, __VA_ARGS__);

// Supported dt formats
const char * DATETIME_FORMATS[] = 
	{"%Y-%m-%dT%H:%M:%S",	/* sip.log: "2017-02-21T14:53:22" */
         "%b %d %H:%M:%S",	/* porta-billing.log: "Oct 30 05:54:01" */ 
         "%Y-%m-%d %H:%M:%S"};	/* web_profiling.log, taskstack.log: 
				   "2017-02-21 14:53:22" */

// Functions declaration
void process_args(int, char [], unsigned long long *, char *);
void usage(char *);
long long unsigned search_start_bit_of_bz2_block(int, bunzip_data *);
// converts char string to epoch time (seconds since Jan 1 1970 00:00:00 UTC)
time_t convert_dateTime_string_to_epoch(char *, char *);
int uncompress_first_buffer_of_block(unsigned long, bunzip_data *, char *);
int uncompress_last_2_buffers_of_block(unsigned long, bunzip_data *, char *);
char * get_first_dateTime_string_from_bz2_block(unsigned long, short, int, bunzip_data *, char *, char *);
char * get_last_dateTime_string_from_bz2_block(unsigned long, short, int, bunzip_data *, char *, char *);
unsigned long opt_from_binary_search(unsigned long, unsigned long, time_t, int, bunzip_data *, char *, int, char *, char *, short);
bool seek_dateTime_string_in_outbuf(char *, int , char *);
int seek_dateTime_string_in_block(unsigned long, bunzip_data *, char *, bool *);
int seek_last_dateTime_string_in_block(unsigned long, bunzip_data *, char *, bool *);
int uncompress_block(unsigned long, bunzip_data *, int);
char * define_dateTime_format(char *, char *);
unsigned long long opt_from_first_block_search(unsigned long long, bunzip_data *, char *);
long long find_last_block_pos(bunzip_data *);
int determine_dateTime_substring_position(short, int, bunzip_data *, char *, char *);


static const struct option long_options[] = {
    { "from", required_argument, NULL, 'b' },
    { "to", required_argument, NULL, 'e' },
    { "file", required_argument, NULL, 'f' },
    { NULL, 0, NULL, 0 }
};


int main(int argc, char *argv[])
{
// Variables declaration:
    int ifd, opt, status, opt_from_length;
    long file_size;
    char * opt_from, * opt_to, * opt_input_file;	// options --from, --to, --file
    bunzip_data *bd;
    unsigned long long opt_from_pos, opt_to_pos, next_block_pos;	// bit position of start of block where opt_from string was found
    unsigned long long cur_rel_bz2_block_pos, cur_abs_bz2_block_pos, abs_opt_from_pos;
    time_t opt_from_time_t, opt_to_time_t, first_dateTime_string_in_outbuf_time_t;	// stores the time_t values of opt_from, opt_to strings
    //char * outbuf;
    char * opt_from_dateTime_format;
    char * opt_to_dateTime_format;
    char * dateTime_format;
    bool is_dateTime_string_found = false;
    long long lseek_result;
    char * file_first_date, * file_last_date;	// first/last dates in the input file
    time_t file_first_date_time_t, file_last_date_time_t;	// first/last dates converted to time_t
    short dateTime_substring_pos;	// position of the first character of dateTime_substring in a string


// Parse the options and assign its values to variables
    while ((opt = getopt_long(argc, argv, "", long_options, NULL)) != -1)
    {
        //printf("opt = %c\n", opt);
      
        switch (opt)
	{
            case 'b':
                opt_from = optarg;
		//printf("main(): opt_from = %s, &opt_from = %x\n", opt_from, &opt_from);
                break;
            case 'e':
                opt_to = optarg;
		//printf("main(): opt_to = %s, &opt_to = %x\n", opt_to, &opt_to);
                break;
            case 'f':
                opt_input_file = optarg;
                break;
            default: /* '?' */
		usage(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

// Open an input bz2 file
    if ((ifd = open(opt_input_file ,O_RDONLY)) < 0)
    {
	//printf("main(), ERROR: open(opt_input_file ,O_RDONLY) < 0\n");
	error_print("Can't open the file %s\n" \
		    "%s\n", opt_input_file, strerror(errno));
	exit(EXIT_FAILURE);
    }

// Check if the input file is in bzip2 format, prepare bd structure for work.
    if (status = start_bunzip( &bd, ifd, 0, 0))
    {
        printf("main(), ERROR: start_bunzip() returned %s\n", bunzip_errors[-status] );
	exit(EXIT_FAILURE);
    }

// Define dt formats of --from and --to arguments and check if they are equal.
    opt_from_dateTime_format = define_dateTime_format(opt_from, opt_from_dateTime_format);
    //printf("%s(): opt_from = \"%s\", dt format = \"%s\"\n", __func__, opt_from, opt_from_dateTime_format);
    opt_to_dateTime_format = define_dateTime_format(opt_to, opt_to_dateTime_format);
    //printf("%s(): opt_to =   \"%s\", dt format = \"%s\"\n", __func__, opt_to, opt_to_dateTime_format);
    // Check if opt_from dt format equals to opt_to dt format
    if (strcmp(opt_from_dateTime_format, opt_to_dateTime_format) != 0)
    {
	error_print("%s\n", "Values of --from and --to were not set in same datetime format");
        exit(EXIT_FAILURE);
    }
    dateTime_format = opt_from_dateTime_format;
    //exit(EXIT_SUCCESS);

    //printf("main(): ifd = %d\n", ifd);

// Convert opt_from, opt_to strings to epoch values to make it possible to be compared arithmetically
    opt_from_time_t = convert_dateTime_string_to_epoch(opt_from, dateTime_format);
    //printf("main():\topt_from =\t%s\topt_from_time_t =\t%d\n", opt_from, opt_from_time_t);
    opt_to_time_t = convert_dateTime_string_to_epoch(opt_to, dateTime_format);
    //printf("main():\topt_to =\t%s\topt_to_time_t =\t\t%d\n", opt_to, opt_to_time_t);

    opt_from_length = strlen(opt_from);

// Check if opt_from >= opt_to
    if ( opt_from_time_t >= opt_to_time_t )
    {
        error_print("%s\n", "Value of --from shouldn't be >= value of --to");
        exit(EXIT_FAILURE);
    }

    char first_dateTime_string_in_outbuf[opt_from_length];      // first dt string which is started from newline and was found in an output buffer
    char last_dateTime_string_in_outbuf[opt_from_length];      // last dt string that was found in the last output buffer

// Check if opt_from, opt_to are not outside the period, covered by an input file
    // determine the position of dateTime_string in a file
    // and get the first dateTime_substring from the first block of a file
    dateTime_substring_pos = determine_dateTime_substring_position(FIRST_BLOCK_POS, opt_from_length, bd, first_dateTime_string_in_outbuf, dateTime_format);
    file_first_date = first_dateTime_string_in_outbuf;
    debug_print("file_first_date (block %d) is: %s", FIRST_BLOCK_POS, file_first_date);
    file_first_date_time_t = convert_dateTime_string_to_epoch(file_first_date, dateTime_format);
    //printf("%s(): file_first_date_time_t = %d\n", __func__, file_first_date_time_t);

    if (opt_from_time_t < file_first_date_time_t)
    {
	error_print("A value of --from shouldn't be < the first date in the file "
        	    "(%s)", file_first_date);
	exit(EXIT_FAILURE);
    }

    // get the last datetime value from the last block of a file
    cur_rel_bz2_block_pos = find_last_block_pos(bd);
    //printf("%s: last block position = %llu\n", __func__, cur_rel_bz2_block_pos + bd->cur_file_offset * 8);
    //printf("%s: file_last_date (block %llu) is: %s\n", __func__, cur_rel_bz2_block_pos, file_first_date);
    file_last_date = get_last_dateTime_string_from_bz2_block(cur_rel_bz2_block_pos, dateTime_substring_pos, opt_from_length, bd, last_dateTime_string_in_outbuf, dateTime_format);
    debug_print("file_last_date (block %llu) is: %s\n", bd->cur_file_offset * 8 + cur_rel_bz2_block_pos, file_last_date);
    file_last_date_time_t = convert_dateTime_string_to_epoch(file_last_date, dateTime_format);
    //printf("%s(): file_last_date_time_t = %d\n", __func__, file_last_date_time_t);

    if (opt_to_time_t > file_last_date_time_t)
    {
	error_print("A value of --to shouldn't be > the last date in the file "
		    "(%s)", file_last_date);
        exit(EXIT_FAILURE);
    }

    bd->in_fd = ifd;

// Define a file size 
    file_size = lseek(bd->in_fd, 0, SEEK_END);
    //printf("main(): lseek position = %lu\n", lseek(ifd, 0, SEEK_CUR ));

// Search a block where opt_from is located
    opt_from_pos = opt_from_binary_search(0, file_size, opt_from_time_t, ifd, bd, opt_from, opt_from_length,
                                          first_dateTime_string_in_outbuf,  dateTime_format, dateTime_substring_pos);

    if (opt_from_pos + bd->cur_file_offset * 8 != FIRST_BLOCK_POS)
    {
// Search for the very first block where opt_from is located
        opt_from_pos = opt_from_first_block_search(opt_from_pos, bd, opt_from);
        //printf("%s(): The very first opt_from_pos = %llu\n", __func__, bd->cur_file_offset * 8 + opt_from_pos);
        //printf("opt_from_pos + bd->cur_file_offset * 8 = %lu\n", opt_from_pos + bd->cur_file_offset * 8);
    }

// Save current relative position
    cur_rel_bz2_block_pos = opt_from_pos;

    //exit(EXIT_SUCCESS);

// Set first_dateTime_string_in_outbuf_time_t of the first bz2 block, where opt_from was found, to opt_from_time_t 
    first_dateTime_string_in_outbuf_time_t = opt_from_time_t;

    //printf("main(): first_dateTime_string_in_outbuf_time_t = %llu, opt_to_time_t = %llu\n", first_dateTime_string_in_outbuf_time_t, opt_to_time_t);

// Uncompress the first block, where opt_from was found, and all the next blocks till the last one where first found dt sting = opt_to
    while (first_dateTime_string_in_outbuf_time_t <= opt_to_time_t)
    {
    // Uncompress a block
#if !DEBUG
	    uncompress_block(cur_rel_bz2_block_pos, bd, ifd);
	//printf("%llu\n", bd->cur_file_offset * 8 + cur_rel_bz2_block_pos);
#endif
    // Move file offset to the byte next to the one where current bz2 block was found
	bd->cur_file_offset = lseek(ifd, bd->cur_file_offset + cur_rel_bz2_block_pos / 8 + 1, SEEK_SET);
	//printf("main(): lseek(ifd, bd->cur_file_offset + cur_rel_bz2_block_pos / 8 + 1, SEEK_SET) = %llu\n", bd->cur_file_offset);

    // Search bit position of the next block
	cur_rel_bz2_block_pos = search_start_bit_of_bz2_block(ifd, bd);
	//printf("main(): next block position = %llu, bd->cur_file_offset = %llu\n", cur_rel_bz2_block_pos, bd->cur_file_offset);

    // Uncompress the block and find the first dt sting there
	get_first_dateTime_string_from_bz2_block(cur_rel_bz2_block_pos, dateTime_substring_pos, opt_from_length, bd, first_dateTime_string_in_outbuf, dateTime_format);
	//printf("main(): first_dateTime_string_in_outbuf in the block %llu = %s\n", bd->cur_file_offset * 8 + cur_rel_bz2_block_pos, first_dateTime_string_in_outbuf);

    // Convert previously found dt string and convert it into epoch time format
	first_dateTime_string_in_outbuf_time_t = convert_dateTime_string_to_epoch(first_dateTime_string_in_outbuf, dateTime_format);
	//printf("main(): first_dateTime_string_in_outbuf_time_t = %d\n", first_dateTime_string_in_outbuf_time_t);
    }


// Check if opt_to exists in a whole block
    status = seek_dateTime_string_in_block(cur_rel_bz2_block_pos, bd, opt_to, &is_dateTime_string_found);
    if (status)
    {
	error_print("seek_dateTime_string_in_block() returned %s", bunzip_errors[-status]);
	exit(EXIT_FAILURE);
    }

    //printf("%s(): is_dateTime_string_found = %d\n", __func__, is_dateTime_string_found);

// Find the last block where opt_to exists
    while (is_dateTime_string_found)
    {
        debug_print("opt_to value %s was found in the block %llu", opt_to, cur_rel_bz2_block_pos + bd->cur_file_offset * 8);
	bd->cur_file_offset = lseek(ifd, bd->cur_file_offset + cur_rel_bz2_block_pos / 8 + 1, SEEK_SET);
	cur_rel_bz2_block_pos = search_start_bit_of_bz2_block(ifd, bd);
	seek_dateTime_string_in_block(cur_rel_bz2_block_pos, bd, opt_to, &is_dateTime_string_found);
    }

    //printf("main(): opt_to value was not found in the block %llu\n", cur_rel_bz2_block_pos + bd->cur_file_offset * 8);

// Print a newline at the end
    printf("\n");

    if ( status )
        fprintf( stderr, "\n%s\n", bunzip_errors[-status] );

    return 0;
}


long long find_last_block_pos(bunzip_data *bd)
{
    long long last_block_pos = 0;
    int backward_offset_step = 512;
    int backward_offset;

    // searching for the last bz2 block from file's end by backward_offset  
    for (backward_offset = backward_offset_step; last_block_pos == 0; backward_offset += backward_offset_step)
    {
        bd->cur_file_offset = lseek(bd->in_fd, -backward_offset, SEEK_END);
        // search for the block
        last_block_pos = search_start_bit_of_bz2_block(bd->in_fd, bd);
        //printf("%s: last block position = %llu\n", __func__, last_block_pos + bd->cur_file_offset * 8);
    }

    return last_block_pos;
}


unsigned long long opt_from_first_block_search(unsigned long long opt_from_pos, bunzip_data *bd, char * opt_from)
{
    long long backward_offset_step, cur_rel_bz2_block_pos;
    long long prev_rel_bz2_block_pos, cur_abs_bz2_block_pos, prev_abs_bz2_block_pos, abs_backward_offset_B;
    long long lseek_result;
    int status;
    bool is_dateTime_string_found = true;
    long prev_file_offset;


    backward_offset_step = 0;
    cur_rel_bz2_block_pos = prev_rel_bz2_block_pos = opt_from_pos;
    cur_abs_bz2_block_pos = prev_abs_bz2_block_pos = bd->cur_file_offset * 8 + opt_from_pos;

    //printf("opt_from_first_block_search(): current file offset start of a function = %llu\n", lseek(bd->in_fd, 0, SEEK_CUR));

    //printf("opt_from_first_block_search(): INFO. abs_opt_from_pos = %llu b\n", abs_opt_from_pos);
    //printf("opt_from_first_block_search(lseek(INFO)): %llu B\n", lseek(bd->in_fd, 0, SEEK_SET));

// Search for the previous block
// Set file offset BUFFER_SIZE bytes backward
    while (is_dateTime_string_found == true)
    {
        // Preserve cur_file_offset for prev_rel_bz2_block_pos
        prev_file_offset = bd->cur_file_offset;

	// Loop to find the previous block. 
        while (cur_abs_bz2_block_pos == prev_abs_bz2_block_pos)
        {

            if ((bd->cur_file_offset - BUFFER_SIZE) <= 0) {
		bd->cur_file_offset = lseek(bd->in_fd, 4, SEEK_SET);
	    }
	    else {
                // Set file position to -BUFFER_SIZE byte
                bd->cur_file_offset = lseek(bd->in_fd, -BUFFER_SIZE, SEEK_CUR);
	    }

	    //printf("%s: lseek offset = %lld\n", __func__, bd->cur_file_offset);
            if (bd->cur_file_offset == -1)
            {
                error_print("bd->cur_file_offset = lseek(bd->in_fd, -BUFFER_SIZE, SEEK_CUR) = %lu", bd->cur_file_offset);
                exit(EXIT_FAILURE);
            }
            //printf("opt_from_first_block_search(): lseek(bd->in_fd, -%d, SEEK_CUR) = %llu\n", BUFFER_SIZE, bd->cur_file_offset);

            // Searching for the block starting from the abs_backward_offset_B byte
            cur_rel_bz2_block_pos = search_start_bit_of_bz2_block(bd->in_fd, bd);
	    // Convert found block position into absolute value
            cur_abs_bz2_block_pos = bd->cur_file_offset * 8 + cur_rel_bz2_block_pos;

	    //printf("search_start_bit_of_bz2_block(): cur_rel_bz2_block_pos %lli, prev_rel_bz2_block_pos %lli\n\n", cur_rel_bz2_block_pos, prev_rel_bz2_block_pos);
	    //printf("opt_from_first_block_search(): current file offset = %llu\n", lseek(bd->in_fd, 0, SEEK_CUR));
            //printf("opt_from_first_block_search: cur_abs_bz2_block_pos = %lli b, prev_abs_bz2_block_pos = %lli b\n",\
			                                                  cur_abs_bz2_block_pos, prev_abs_bz2_block_pos);
	    //printf("%s: prev_file_offset = %lu\n", __func__, prev_file_offset);
        }

        // Check if opt_from exists in the current block
        status = seek_dateTime_string_in_block(cur_rel_bz2_block_pos, bd, opt_from, &is_dateTime_string_found);
        if (status)
        {
            error_print("seek_dateTime_string_in_block(ERROR): %s", bunzip_errors[-status]);
            exit(EXIT_FAILURE);
        }
	lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET);
        // If opt_from is found in current block then save it in prev_abs_bz2_block_pos and search for the previous block
	if (is_dateTime_string_found == true)
	{
            prev_abs_bz2_block_pos = cur_abs_bz2_block_pos;
	    prev_rel_bz2_block_pos = cur_rel_bz2_block_pos;

	    // If current block is the first block (located in FIRST_BLOCK_POSnd bit) then stop searching for a previous block
	    debug_print("cur_abs_bz2_block_pos = %lu", cur_abs_bz2_block_pos);
	    if (cur_abs_bz2_block_pos == FIRST_BLOCK_POS) {
                prev_file_offset = 4;
                break;
	    }
	}
    }
    //printf("opt_from_first_block_search: cur_rel_bz2_block_pos = %lli b, prev_rel_bz2_block_pos = %lli\n", cur_rel_bz2_block_pos, prev_rel_bz2_block_pos);
    //printf("opt_from_first_block_search: cur_abs_bz2_block_pos = %lli b, prev_abs_bz2_block_pos = %lli\n", cur_abs_bz2_block_pos, prev_abs_bz2_block_pos);

    bd->cur_file_offset = lseek(bd->in_fd, prev_file_offset, SEEK_SET);
    //printf("opt_from_first_block_search(INFO): return %lli cur_file_offset = %lli\n", prev_rel_bz2_block_pos, bd->cur_file_offset);
    return prev_rel_bz2_block_pos;
}


char * define_dateTime_format(char * dateTime_string, char * dateTime_format)
{
    struct tm dateTime_tm;
    short dateTime_formats_counter, dateTime_formats_index, dateTime_formats_array_size;
    short dateTime_format_string_length;


// Define a dt format
    // 1. know an amount of supported dt formats (amount of elements in DATETIME_FORMATS[])
    dateTime_formats_array_size = sizeof(DATETIME_FORMATS) / sizeof(DATETIME_FORMATS[0]);
    //printf("DATETIME_FORMATS size = %d\n", dateTime_formats_array_size);
    // 2. check if opt_from, opt_to strings correspond to one of supported dt formats
    dateTime_formats_counter = 0;
    while (dateTime_formats_counter < dateTime_formats_array_size)
    {
    // Check if a given dateTime_sting corresponds to one of supported dt formats
        if (strptime(dateTime_string, DATETIME_FORMATS[dateTime_formats_counter], &dateTime_tm) != NULL)
        {
	    //dateTime_format = DATETIME_FORMATS[dateTime_formats_counter];
	    //dateTime_format_string_length = strlen(DATETIME_FORMATS[dateTime_formats_counter]);
	    //printf("define_dateTime_format: dateTime_format = %s\n", DATETIME_FORMATS[dateTime_formats_counter]);
	    //dateTime_format = strncpy(dateTime_format, DATETIME_FORMATS[dateTime_formats_counter], dateTime_format_string_length + 1);
	    //printf("strncpy(dateTime_format, DATETIME_FORMATS[dateTime_formats_counter], dateTime_format_string_length + 1). dateTime_format = %s\n", dateTime_format);
	    return DATETIME_FORMATS[dateTime_formats_counter];
            //printf("_from corresponds to dt format %s\n", DATETIME_FORMATS[dateTime_formats_counter]);
        }
	//printf("define_dateTime_format: dateTime_string = %s\n", dateTime_string);
	dateTime_formats_counter++;
    }
    //printf("define_dateTime_format: dateTime_formats_counter = %d, dateTime_formats_array_size = %d\n", dateTime_formats_counter, dateTime_formats_array_size);

// A given dateTime_sting doesn't correspond to one of supported dt formats. Error.
    error_print("%s", "The value of --from or --to arguments was set in "
		        "unsupported datetime format.\n"
	                "Supported datetime formats are:");
    for (dateTime_formats_counter = 0; dateTime_formats_counter < dateTime_formats_array_size; dateTime_formats_counter++)
        printf("\t%s\n", DATETIME_FORMATS[dateTime_formats_counter]);
    printf("\n");
    exit(EXIT_FAILURE);
}


/* Function does binary search on the file's bytes level (lseek()). Then it searches a bz2 block which is nearest from the current middle byte element.
Then it finds the first dt sting in the found bz2 block, converts it to epoch format and compares it with the opt_from_time_t value to understand where it
should continue to search the next middle byte. *****/
unsigned long opt_from_binary_search(unsigned long low, unsigned long high, time_t opt_from_time_t, int ifd, bunzip_data *bd, char *opt_from, int dateTime_length, char * first_dateTime_string_in_outbuf, char * dateTime_format, short dateTime_substring_pos)
{
    unsigned long long mid, middle_pos, abs_middle_pos;
    unsigned int middle_pos_in_bytes;
    time_t middle_time_t;
    time_t first_dateTime_string_in_outbuf_time_t;
    time_t last_dateTime_string_in_block_time_t;
    char last_dateTime_string_in_block[dateTime_length];
    unsigned long long lseek_result;


    while (low <= high)
    {
        mid = low + (high - low) / 2;
	//printf("\n%s: low = %lu, mid = %lu, hig = %lu\n", __func__, low, mid, high);
	if (DEBUG) putchar('\n');
	debug_print("low = %lu, mid = %lu, hig = %lu", low, mid, high);
        // Set file offset to mid byte
        bd->cur_file_offset = lseek(ifd, mid, SEEK_SET);
        if (bd->cur_file_offset != mid){
            printf("%s(): ERROR. lseek(bd->in_fd, mid, SEEK_SET) returned %llu which is not equal to mid (%llu)\n", __func__, bd->cur_file_offset, mid);
            exit(EXIT_FAILURE);
        }

        //printf("opt_from_binary_search: lseek position = %lu\n", lseek(ifd, 0, SEEK_CUR ));
        middle_pos = search_start_bit_of_bz2_block(ifd, bd);
	abs_middle_pos = bd->cur_file_offset * 8 + middle_pos; // absolute value of a middle block position
        //printf("%s: abs_middle_pos = %lu\n", __func__, abs_middle_pos); 

	debug_print("block %llu", abs_middle_pos);

        // Get the first dt string from current block and convert it to epoch time
	get_first_dateTime_string_from_bz2_block(middle_pos, dateTime_substring_pos, dateTime_length, bd, first_dateTime_string_in_outbuf, dateTime_format);
	debug_print("first_dateTime_string_in_outbuf = %s", first_dateTime_string_in_outbuf);
	first_dateTime_string_in_outbuf_time_t = convert_dateTime_string_to_epoch(first_dateTime_string_in_outbuf, dateTime_format);
	//printf("opt_from_binary_search: first_dateTime_string_in_outbuf_time_t = %lu\n", first_dateTime_string_in_outbuf_time_t);
	
        // Get the last dt string from the block and convert it to epoch time
        get_last_dateTime_string_from_bz2_block(middle_pos, dateTime_substring_pos, dateTime_length, bd, last_dateTime_string_in_block, dateTime_format);
	debug_print("last_dateTime_string_in_block = %s", last_dateTime_string_in_block);
        last_dateTime_string_in_block_time_t = convert_dateTime_string_to_epoch(last_dateTime_string_in_block, dateTime_format);
	//printf("opt_from_binary_search: last_dateTime_string_in_block_time_t = %lu\n", last_dateTime_string_in_block_time_t);

	//printf("%s: opt_from_time_t = %lu\n", __func__, opt_from_time_t);
	//printf("%s: opt_from_time_t(%lu) - first_dateTime_string_in_outbuf_time_t(%lu) = %lu\n", __func__, opt_from_time_t, first_dateTime_string_in_outbuf_time_t, opt_from_time_t - first_dateTime_string_in_outbuf_time_t);
	if ( opt_from_time_t > first_dateTime_string_in_outbuf_time_t ) {

            debug_print("%s: opt_from (%s) > first_dateTime_string_in_outbuf (%s)\n", __func__, opt_from, first_dateTime_string_in_outbuf);

            if ( opt_from_time_t <= last_dateTime_string_in_block_time_t ) {
                // Break the loop and return the block position.
                debug_print("opt_from (%s) <= last_dateTime_string_in_block (%s)", opt_from, last_dateTime_string_in_block);
                break;
            }
	    
	    // Set low to the byte next to the current byte where current bz2 block was found.
            // So we convert middle_pos from bits to bytes by / 8 and add 1
            //low = mid + middle_pos / 8 + 1;
            low = mid + 1;

	} else if (opt_from_time_t < first_dateTime_string_in_outbuf_time_t) {

            debug_print("opt_from (%s) < first_dateTime_string_in_outbuf (%s)\n", opt_from, first_dateTime_string_in_outbuf);

            // Set high to the byte previous to the current byte where current bz2 block was found.
            // So we convert middle_pos from bits to bytes by / 8 and subtract 1 
	    //high = mid + middle_pos / 8 - 1;
	    high = mid - 1;

	} else {
            // If opt_from == first_dateTime_string_in_outbuf_time_t then break the loop and return current block position
	    printf("opt_from (%s) == first_dateTime_string_in_outbuf (%s)", opt_from, first_dateTime_string_in_outbuf);

            //printf("opt_from_binary_search: opt_from was found in the block with starting bit %llu\n\n", middle_pos + bd->cur_file_offset * 8);
            lseek_result = lseek(ifd, bd->cur_file_offset, SEEK_SET); // to uncompress the block in which opt_from was found it's needed to move lseek to the previous position
            if (lseek_result != bd->cur_file_offset)
            {
                printf("\nERROR: line %d, function %s():\n"
		"lseek(ifd, bd->cur_file_offset, SEEK_SET) returned %llu, which is not equal "
	       	"to bd->cur_file_offset(%llu)\n\n",
		__LINE__, __func__, lseek_result, bd->cur_file_offset);
                exit(EXIT_FAILURE);
            }

            break;
	}
            
    }

    return middle_pos;
}


/*
 * Seek the bunzip_data `bz` to a specific position in bits `pos` by lseeking
 * the underlying file descriptor and priming the buffer with appropriate
 * bits already consumed. This probably only makes sense for seeking to the
 * start of a compressed block.
 */
unsigned int seek_bits( bunzip_data *bd, unsigned long pos )
{
    long n_byte = pos / 8;
    char n_bit = pos % 8;
    //printf("seek_bits: pos = %lu, n_byte = %d, n_bit = %d\n", pos, n_byte, n_bit);

    //printf("seek_bits: lseek position = %lu, n_byte(%lu) + bd->cur_file_offset(%lu)\n", lseek(bd->in_fd, 0, SEEK_CUR), n_byte, bd->cur_file_offset);
 
    // Seek the underlying file descriptor
    if ( (lseek( bd->in_fd, n_byte, SEEK_CUR)) != n_byte + bd->cur_file_offset) // + bd->cur_file_offset because lseek returns the amount of bytes from the beginning of a file
    //if ( (lseek( bd->in_fd, n_byte, SEEK_CUR)) != n_byte) // + bd->cur_file_offset because lseek returns the amount of bytes from the beginning of a file
    {
        //printf("seek_bits: current file offset AFTER lseek = %ld\n", lseek(bd->in_fd, 0, SEEK_CUR));
        //printf("seek_bits: bd->lseek_position = lseek( bd->in_fd, n_byte, SEEK_CUR) = %ld\n", bd->lseek_position);
	error_print("lseek( bd->in_fd, n_byte(%lu), SEEK_CUR)) != n_byte(%lu) + bd->cur_file_offset(%lu)", n_byte, n_byte, bd->cur_file_offset);
        exit(EXIT_FAILURE);
	//return -1;
    }

    bd->inbufBitCount = bd->inbufPos = bd->inbufCount = 0;

    get_bits( bd, n_bit );

    return pos;
}


int uncompress_first_buffer_of_block(unsigned long pos, bunzip_data *bd, char *outbuf)
{
    int status;
    int gotcount = 0;


    seek_bits( bd, pos );

    /* Fill the decode buffer for the block */
    if ((status = get_next_block( bd ))) {
        printf("%s: ERROR. get_next_block() returned %d, bunzip_errors[-status]\n", __func__, status);
        exit(EXIT_FAILURE);
    }

    /* Init the CRC for writing */
    bd->writeCRC = 0xffffffffUL;

    /* Zero this so the current byte from before the seek is not written */
    bd->writeCopies = 0;

    /* Decompress the first buffer of block */
    gotcount = read_bunzip( bd, outbuf, BUFFER_SIZE );

    lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET);

    if ( gotcount < 0 ) {
        status = gotcount;
	printf("%s: ERROR. read_bunzip() returned %d, %s\n", __func__, gotcount, bunzip_errors[-status]);
	exit(EXIT_FAILURE);
    }
    else if ( gotcount == 0 ) {
	status = gotcount;
        printf("%s: ERROR. read_bunzip() returned %d, %s\n", __func__, gotcount, bunzip_errors[-status]);
        exit(EXIT_FAILURE);
    }

    return gotcount;
}


int uncompress_last_2_buffers_of_block(unsigned long pos, bunzip_data *bd, char *two_last_outbufs)
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
    if ((status = get_next_block( bd ))) {
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
        gotcount = read_bunzip( bd, last_outbuf, BUFFER_SIZE );

	// Sum up all gotcounts
	totalcount += gotcount;

        //lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET);

        if ( gotcount < 0 ) {
            status = gotcount;
            printf("%s: ERROR. read_bunzip() returned %d, %s\n", __func__, gotcount, bunzip_errors[-status]);
            exit(EXIT_FAILURE);
        }
	else if (gotcount == BUFFER_SIZE) {
            // If gotcount == BUFFER_SIZE then this is not the last buffer. So store it into prev_outbuf
	    memcpy (prev_outbuf, last_outbuf, BUFFER_SIZE);
	    prev_gotcount = gotcount;
	}
	else if (gotcount > 0 && gotcount < BUFFER_SIZE) {
            // if outbuf was written not in full then this is the last buffer.
	    // So store the content of prev_outbuf and last_outbuf into two_last_oubufs
	    last_gotcount = gotcount;
	    memcpy(&two_last_outbufs[0], prev_outbuf, sizeof(char) * prev_gotcount);
	    memcpy(&two_last_outbufs[BUFFER_SIZE], last_outbuf, sizeof(char) * last_gotcount);
            // Put null char after data end
	    two_last_outbufs[prev_gotcount + last_gotcount] = '\0';
            break;
	}
	else if (gotcount == 0 && totalcount == 0) {
/*        else if ( gotcount == 0 ) { // if gotcount == 0 outbuf is not rewritten, so it contains previous buffer data
            if ( prev_gotcount > 0 ) {// if prev_gotcount is not empty this outbuf is the last one in this block
                break;
            } */
	    // If the first read_bunzip() wrote 0 bytes into last_outbuf, exit with error.
	    status = gotcount;
	    printf("%s(): read_bunzip() returned %d uncompressing the block %llu. %s\n", __func__, gotcount, bd->cur_file_offset * 8 + pos, bunzip_errors[-status]);
            exit(EXIT_FAILURE);
        }
    }

    //printf("%s: outbuf = %s\n", __func__, outbuf);
    return prev_gotcount + last_gotcount;
}


int seek_dateTime_string_in_block(unsigned long pos, bunzip_data *bd, char * opt_from, bool * is_dateTime_string_found)
{
    int status = 0, i = 0;
    int gotcount = 0;
    char outbuf[BUFFER_SIZE];
    int seek_bits_return;


    //printf("seek_dateTime_string_in_block: &bd = %p, bd = %p\n", &bd, bd);
    
        seek_bits_return = seek_bits( bd, pos );
        //printf("seek_dateTime_string_in_block: seek_bits returned %d\n", seek_bits_return);
        //printf("seek_dateTime_string_in_block: opt_from = %s\n", opt_from);

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
            gotcount = read_bunzip( bd, outbuf, BUFFER_SIZE );
	    //printf("seek_dateTime_string_in_block: read_bunzip returned %d\n", gotcount);
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
                // Here we have uncrompressed data in outbuf

                //printf("seek_dateTime_string_in_block: iteration %d\n", i);
                // Search from/to strings in outbuf. As soon as stings was found - stop checking further blocks.
                if (*is_dateTime_string_found = seek_dateTime_string_in_outbuf(opt_from, gotcount, outbuf)) {
		    //printf("seek_dateTime_string_in_block: is_dateTime_string_found = %d\n", is_dateTime_string_found);
		    goto seek_bunzip_finish;
		}
                //printf("seek_dateTime_string_in_block: iteration %d, gotcount = %d\n", i, gotcount);


                //write( 1, outbuf, gotcount );
                //exit(EXIT_SUCCESS);
            }
        }

seek_bunzip_finish:

//    if ( bd->dbuf ) free( bd->dbuf );
//    free( bd );

    return status;
}

int seek_last_dateTime_string_in_block(unsigned long pos, bunzip_data *bd, char * opt_from, bool * is_dateTime_string_found)
{
    int status = 0, i = 0;
    int gotcount = 0;
    char outbuf[BUFFER_SIZE];
    int seek_bits_return;


    //printf("seek_dateTime_string_in_block: &bd = %p, bd = %p\n", &bd, bd);
    
        seek_bits_return = seek_bits( bd, pos );
        //printf("seek_dateTime_string_in_block: seek_bits returned %d\n", seek_bits_return);
        //printf("seek_dateTime_string_in_block: opt_from = %s\n", opt_from);

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
            gotcount = read_bunzip( bd, outbuf, BUFFER_SIZE );
	    //printf("seek_dateTime_string_in_block: read_bunzip returned %d\n", gotcount);
            if ( gotcount < 0 )
            {
                status = gotcount;
                break;
            }
            else if ( gotcount == 0 )
            {
                break;
            }
	    else if ( gotcount == BUFFER_SIZE )  // if gotcount == BUFFER_SIZE then we suppose this is not the last buffer of block. So continue to the next buffer.
	    {
	        continue;
	    }
            else
            {
                // Here we have an uncompressed data in the last buffer

                //printf("seek_dateTime_string_in_block: iteration %d\n", i);
                // Search from/to strings in outbuf. As soon as stings was found - stop checking further blocks.
                //if (*is_dateTime_string_found = seek_dateTime_string_in_outbuf(opt_from, gotcount, outbuf)) {
		    //printf("seek_dateTime_string_in_block: is_dateTime_string_found = %d\n", is_dateTime_string_found);
		//    goto seek_bunzip_finish;
		//}
                //printf("seek_dateTime_string_in_block: iteration %d, gotcount = %d\n", i, gotcount);


                write( 1, outbuf, gotcount );
                //exit(EXIT_SUCCESS);
            }
        }

seek_bunzip_finish:

//    if ( bd->dbuf ) free( bd->dbuf );
//    free( bd );

    return status;
}


/*** Function determines a position of dt substring in the first bz2 block, in the outbuf ***/
int determine_dateTime_substring_position(short pos, int test_string_len, bunzip_data *bd, char * first_dateTime_string_in_outbuf, char * dateTime_format)
{
    int outbuf_byte_pos, test_string_byte_pos;
    struct tm dateTime_tm = {0}; // dt data structures to which opt_from, opt_to should be converted
    char test_string [test_string_len]; // sting with the same size as opt_from/opt_to to check if it matches to dateTime_format
    char outbuf[BUFFER_SIZE]; // first outbuf from which first dt string should be searched
    int gotcount = 0;	// amount of bytes which was uncompressed read to outbuf
    int i;
    int dateTime_substring_position;	// position of the first char of dateTime_substring in a string

    
    //printf("get_first_dateTime_string_from_bz2_block: bd->cur_file_offset = %lu, pos = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR), pos);

// Get first uncompressed outbuf from a block
    gotcount = uncompress_first_buffer_of_block(pos, bd, outbuf);
    if (gotcount <= 0)
    {
        printf("%s: ERROR. the function uncompress_first_buffer_of_block() returned the value %d which is <= 0\n", __func__, gotcount);
        exit(EXIT_FAILURE);
    } 
    else if (gotcount > BUFFER_SIZE)
    {
        printf("%s: ERROR. the function uncompress_first_buffer_of_block() returned the value %d which is greater than %d (BUFFER_SIZE).\n", __func__, gotcount, BUFFER_SIZE);
	exit(EXIT_FAILURE);
    }

    // loop through the first string of outbuf
    for (outbuf_byte_pos = 0; outbuf[outbuf_byte_pos] != '\n'; outbuf_byte_pos++) {
	//printf("%s: %c", __func__, outbuf[outbuf_byte_pos]);
        //if (outbuf[outbuf_byte_pos] == '\n') outbuf_byte_pos++;     
	for (test_string_byte_pos = 0, i = outbuf_byte_pos; test_string_byte_pos < test_string_len; test_string_byte_pos++) {
	    test_string[test_string_byte_pos] = outbuf[i++];
	}

        // add null char to the end of array to make it C string
        test_string[test_string_byte_pos] = '\0';
 
        //printf("%s: test_string = %s\n", __func__, test_string);
	//putchar(outbuf[outbuf_byte_pos]);
	
	// if test_string corresponds to a dateTime_format,  
	// save the first char position and copy it to first_dateTime_string_in_outbuf
	if ((strptime(test_string, dateTime_format, &dateTime_tm)) != NULL) {
	    dateTime_substring_position = outbuf_byte_pos;
	    //printf("%s: test_string = %s, test_string starts from %d character in the first string\n", __func__, test_string, dateTime_substring_position);
            strncpy(first_dateTime_string_in_outbuf, test_string, test_string_len + 1); //copy test_string to first_dateTime_string_in_outbuf
            return dateTime_substring_position;
	}
    }

    error_print("Test string \"%s\" was not found int the first string of the file", test_string);
    exit(EXIT_FAILURE);
}

// Function gets the first dt string which corresponds to any of known date formats
char * get_first_dateTime_string_from_bz2_block(unsigned long pos, short dateTime_substring_pos, int test_string_len, bunzip_data *bd, char * first_dateTime_string_in_outbuf, char * dateTime_format)
{
    int outbuf_byte_pos, test_string_byte_pos;
    struct tm dateTime_tm = {0}; // dt data structures to which opt_from, opt_to should be converted
    char test_string [test_string_len]; // sting with the same size as opt_from/opt_to to check if it matches to dateTime_format
    char outbuf[BUFFER_SIZE]; // first outbuf from which first dt string should be searched
    int gotcount;	// amount of bytes which was uncompressed read to outbuf
    int cur_line = 0;	// current line numberf
    int test_string_offset; // position of the first character of test_string relatively to '\n' in outbuf
    int str_char_pos; // position of char in a string (starting from next char to \n)
    int min_str_len; // minimum string length
    bool str_len_enough; // flag to check if string length > min_str_len
    time_t dateTime_time_t;


//printf("%s: pos = %u, bd->cur_file_offset = %lu, abs_pos = %lu\n", __func__, pos, bd->cur_file_offset, bd->cur_file_offset * 8 + pos);

// Clean first_dateTime_string_in_outbuf from the previous value
    first_dateTime_string_in_outbuf[0] = 0;

// Get the first uncompressed outbuf from a block
    gotcount = uncompress_first_buffer_of_block(pos, bd, outbuf);

// Loop through outbuf
    for (outbuf_byte_pos = 0; outbuf_byte_pos < gotcount; outbuf_byte_pos++) {

        //printf("outbuf[%d] = %c\n", outbuf_byte_pos, outbuf[outbuf_byte_pos]);

        if (outbuf[outbuf_byte_pos] != '\n')
            continue;

        // Check if current string's length > dateTime_substring_pos + test_string_len
        min_str_len = dateTime_substring_pos + test_string_len;
        str_len_enough = true;
        //printf("%s: min_str_len = %d\n", __func__, min_str_len);
        for (str_char_pos = 0; str_char_pos < min_str_len; str_char_pos++) {
            if (outbuf[outbuf_byte_pos + 1 + str_char_pos] == '\n') {
                outbuf_byte_pos = outbuf_byte_pos + str_char_pos; // set outbuf_byte_pos to char which is previous to '\n' and continue
								 // because outbuf_byte_pos++ will set outbuf_byte_pos to '\n' again
                str_len_enough = false;
                break; 
            }
        }

        if (str_len_enough == false)
            continue;
     
	for (test_string_byte_pos = 0; test_string_byte_pos < test_string_len; test_string_byte_pos++) {
            test_string_offset = outbuf_byte_pos + 1 + dateTime_substring_pos + test_string_byte_pos;
	    test_string[test_string_byte_pos] = outbuf[test_string_offset];
	}

        // add null char to the end of array to make it C string
        test_string[test_string_byte_pos] = '\0';
 
        //printf("%s: test_string[0] = %c, test_string = %s\n", __func__, test_string[0], test_string);

        // If test_string doesn't match to the dateTime_format (strptime == NULL), go to the next char
        if (!strptime(test_string, dateTime_format, &dateTime_tm)) {
            continue;
        } else {
            // Else, copy test_string to first_dateTime_string_in_outbuf and return it.
            strncpy(first_dateTime_string_in_outbuf, test_string, test_string_len + 1);
            //printf("%s: first_dateTime_string_in_outbuf = %s\n", __func__, first_dateTime_string_in_outbuf);
            break;
        }
    }

    return first_dateTime_string_in_outbuf;
}


// Function searches for the last dt substring in the end of the last bz2 block
char * get_last_dateTime_string_from_bz2_block(unsigned long pos, short dateTime_substring_pos, int test_string_len, bunzip_data *bd, char *last_dateTime_string_in_outbuf, char *dateTime_format)
{
    int outbuf_byte_pos, test_string_byte_pos, last_char_pos;
    struct tm dateTime_tm = {0};	// dt data structures to which opt_from, opt_to should be converted
    char test_string [test_string_len]; // sting with the same size as opt_from/opt_to to check if it matches to dateTime_format
    char two_last_outbufs [BUFFER_SIZE * 2 + 1]; 	// 2 last outbufers from which last dt string should be searched

    int gotcount;			// amount of bytes which was written to outbuf
    time_t dateTime_time_t;
    int str_len, cur_nl_pos, prev_nl_pos;


// Clean last_dateTime_string_in_outbuf from the previous value
    memset(last_dateTime_string_in_outbuf, 0, test_string_len);

// Clean two_last_outbufs from the previous value
    memset(two_last_outbufs, 0, BUFFER_SIZE * 2);
    two_last_outbufs[BUFFER_SIZE * 2] = '\n';

// Get the last uncompressed outbuf from a block
    gotcount = uncompress_last_2_buffers_of_block(pos, bd, two_last_outbufs);
    //printf("%s(): gotcount = %d\n", __func__, gotcount);
    last_char_pos = gotcount - 1;
    prev_nl_pos = last_char_pos;

// Loop through two_last_outbufs in reverse direction
    for (outbuf_byte_pos = last_char_pos; outbuf_byte_pos >= 0; outbuf_byte_pos--) {
	//printf("%c", two_last_outbufs[outbuf_byte_pos]);

	if (two_last_outbufs[outbuf_byte_pos] != '\n')
	    continue;
	
	cur_nl_pos = outbuf_byte_pos;
	str_len = prev_nl_pos - cur_nl_pos - 2;

	if (str_len < 0)
	    str_len = 0;

	//printf("prev_nl_pos = %d, cur_nl_pos = %d, str_len = %d\n", prev_nl_pos, cur_nl_pos, str_len);

	prev_nl_pos = cur_nl_pos;

	// If string length is less than dt substring offset (from the begining of the string) + the length of test_sting
	// then go to the next char (in backward direction).
        if (str_len < (dateTime_substring_pos + test_string_len))
            continue;

        for (test_string_byte_pos = 0; test_string_byte_pos < test_string_len; test_string_byte_pos++) {
            test_string[test_string_byte_pos] = two_last_outbufs[outbuf_byte_pos + 1 + dateTime_substring_pos + test_string_byte_pos];
            //printf("test_string[%d] = two_last_outbufs[%d + 1 + %d + %d] = %c\n", test_string_byte_pos, outbuf_byte_pos, dateTime_substring_pos, test_string_byte_pos, two_last_outbufs[outbuf_byte_pos + 1 + dateTime_substring_pos + test_string_byte_pos]);
	    //printf("test_string = %s\n", test_string);
	}

        // Add null char to the end of the array to make it a C string
        test_string[test_string_len] = '\0';
        
        //printf("%s(): test_string = %s\n", __func__, test_string);

	// If test_string doesn't match to the dateTime_format (strptime == NULL), go to the next char (in backward direction).
	// Else, copy test_string to last_dateTime_string_in_outbuf and return it.
        if (!strptime(test_string, dateTime_format, &dateTime_tm)) {
	    continue;
	} else {
	    // Copy test_string to last_dateTime_string_in_outbuf
	    strncpy(last_dateTime_string_in_outbuf, test_string, test_string_len + 1);
            //printf("%s(): test_string = %s\n", __func__, test_string);
	    //printf("%s: last_dateTime_string_in_outbuf = %s\n", __func__, last_dateTime_string_in_outbuf);
	    break;
	}

    }

    return last_dateTime_string_in_outbuf;
}


bool seek_dateTime_string_in_outbuf(char * dateTime_string, int gotcount, char * outbuf)
{
    int outbuf_byte_pos, dateTime_byte_pos;
    int len_dateTime_string = strlen(dateTime_string);
    char found_dateTime_string[len_dateTime_string];

//    printf("seek_dateTime_string_in_outbuf: dateTime_string = %s\n\n", dateTime_string);

    dateTime_byte_pos = 0;

    // Loop to check every character from outbuf to find dateTime_sting(s)
    for (outbuf_byte_pos = 0; outbuf_byte_pos < gotcount; outbuf_byte_pos++) {

        // Search the substring in a buffer that matches to from argument
        if (outbuf[outbuf_byte_pos] == dateTime_string[dateTime_byte_pos]) {

            //if (outbuf_byte_pos == 0)
	    found_dateTime_string[dateTime_byte_pos] = dateTime_string[dateTime_byte_pos];
	    //printf("found_dateTime_string[%d] = %c\n", dateTime_byte_pos, found_dateTime_string[dateTime_byte_pos]);

            // If the first character was matched and this is the first character of a line - increase
            // dateTime_byte_pos index to check if the next characters are matched on the next iteration
            // the statement '&& outbuf_byte_pos != 0' protects from going before the outbuf
            if (outbuf[outbuf_byte_pos - 1] == '\n' && outbuf_byte_pos > 0) {
                //printf("dateTime_string[%d] = outbuf[%d] = %c\n",
                //        dateTime_byte_pos, outbuf_byte_pos, outbuf[outbuf_byte_pos]);
                dateTime_byte_pos = 1;
                continue;	// go to the next cycle
            }

            // if the first character in a line was already matched continue to check if the rest are also matched
            if (dateTime_byte_pos >= 1 && dateTime_byte_pos <= len_dateTime_string )
	    {
                //printf("seek_dateTime_string_in_outbuf: dateTime_string[%d] = outbuf[%d] = %c\n",
                //        dateTime_byte_pos, outbuf_byte_pos, outbuf[outbuf_byte_pos]);
                dateTime_byte_pos++;

                if (dateTime_byte_pos == len_dateTime_string)
		{
                    //printf("seek_dateTime_string_in_outbuf: match! dateTime_string = %s\n", dateTime_string);
                    return true;
                }
	       
                continue;	// go to the next cycle
            }
        }
        else {
            dateTime_byte_pos = 0;
        }

        //printf("dateTime_byte_pos = %d, outbuf[%d] = %c\n",
        //        dateTime_byte_pos, outbuf_byte_pos, outbuf[outbuf_byte_pos]);
    }

    return false;
}


int uncompress_block(unsigned long pos, bunzip_data *bd, int ifd)
{
    int status = 0, i = 0;
    int gotcount = 0;
    char outbuf[BUFFER_SIZE];


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
        gotcount = read_bunzip( bd, outbuf, BUFFER_SIZE );
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
            // Here we have uncrompressed data in outbuf
            write( 1, outbuf, gotcount );
        }
    }

seek_bunzip_finish:

//    if ( bd->dbuf ) free( bd->dbuf );
//    free( bd );
    if ( status ) fprintf( stderr, "\n%s\n", bunzip_errors[-status] );

    return status;
}


/* Converts char dt string to epoch time (seconds since Jan 1 1970 00:00:00 UTC) */
time_t convert_dateTime_string_to_epoch(char * dateTime_string, char * dateTime_format)
{
    int Y, m, d, H, M; // to store %Y, %m, %d, %H, %M
    float S; // accept float value as seconds
    char a[4], b[4]; // to store %a and %b
    struct tm dateTime_tm = {0}; // dt data structures to which opt_from, opt_to should be converted
    time_t dateTime_time_t;
  

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

    //printf("convert_dateTime_string_to_epoch: dateTime_format = %s, dateTime_string = %s\n", dateTime_format, dateTime_string);
    if ( NULL == strptime(dateTime_string, dateTime_format, &dateTime_tm) )
    {
        error_print("strptime(dateTime_string, dateTime_format, &dateTime_tm) "
		    "can't convert received dateTime_string (\"%s\") into tm "
		    "structure properly", dateTime_string);
	exit(EXIT_FAILURE);
    }

//    printf("%s(): dateTime_tm.tm_sec (Seconds (0-60)) =\t%d\n", __func__, dateTime_tm.tm_sec);
//    printf("%s(): dateTime_tm.tm_min (Minutes (0-59)) =\t%d\n", __func__, dateTime_tm.tm_min);
//    printf("%s(): dateTime_tm.tm_hour (Hours (0-23)) =\t%d\n", __func__, dateTime_tm.tm_hour);
//    printf("%s(): dateTime_tm.tm_mday (Day of the month (1-31)) =\t%d\n", __func__, dateTime_tm.tm_mday);
//    printf("%s(): dateTime_tm.tm_mon (Month (0-11)) =\t%d\n", __func__, dateTime_tm.tm_mon);
//    printf("%s(): dateTime_tm.tm_year (Year - 1900) =\t%d\n", __func__, dateTime_tm.tm_year);

    // convert tm to time_t
    if ((dateTime_time_t = mktime(&dateTime_tm)) == -1) {
        printf("mktime() returned -1. The specified broken-down dt(dateTime_time_t) cannot be represented as calendar time (seconds since Epoch)\n");
        exit(EXIT_FAILURE);
    }
    //printf("convert_dateTime_string_to_epoch: dateTime_time_t = %d\n", dateTime_time_t);
    

    return dateTime_time_t;
}


// Function searches a bit number of the nearest bz2 block starting from current lseek position
long long unsigned search_start_bit_of_bz2_block(int ifd, bunzip_data *bd)
{
    int inbuf_read; 		// amount of bytes which was read to inbuf during one 'read' operation
    unsigned long long inbuf_read_total; 	// amount of bytes which was read from inbuf in total
    unsigned char inbuf[BUFFER_SIZE]; 	// input buffer where data is read from a file
    unsigned long long inbuf_byte_pos; 		// counter for bytes which is reading from ibuf
    unsigned long long hay;	// 64 bits which are read from an input buffer. We search a needle in this hay
    int i;
    unsigned long long needle_shifted[9];		// all possible positions of bits of a needle within a hay
    unsigned long long masks[9];			// all needed variants of masks. A mask ANDs with a hay
    unsigned long long const needle = 0x314159265359;	// magic sequence a new block is started from (BCD (pi))
   							// and compares with a needle
    unsigned long long position; 		// byte number a needle was found

    inbuf_read_total = 0;
    hay = 0;
    position = 0;

    for (i = 0; i <= 8; i++)
    {
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

    //printf("search_start_bit_of_bz2_block: lseek position BEFORE read() = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR));

    // Search a needle starting from every next bit. Read BUFFER_SIZE bytes from input file (with file description ifd) to inbuf buffer.
    while ((inbuf_read = read(ifd, inbuf, BUFFER_SIZE)) > 0)
    {
        //printf("search_start_bit_of_bz2_block: lseek position AFTER read() = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR));
	//printf("inbuf_read = %d\n", inbuf_read);
	
	// Take every byte from inbuf and put it into hay. During every iteration the most right byte of hay is shifted to the left and a new byte from inbuf
	// is put to the hay. Inside hay shift every byte from right to left and is compared to every needle_shifted.
	for (inbuf_byte_pos = 0; inbuf_byte_pos < inbuf_read; inbuf_byte_pos++)
	{
//	    printf("search_start_bit_of_bz2_block: inbuf_read_total = %d\n", inbuf_read_total);
//          printf("hay = (hay << 8) | inbuf[%d] = \t", inbuf_byte_pos);
            
            // Shift the content of hay one byte to the left and put a new byte from inbuf to the most right position
	    hay = (hay << 8) | inbuf[inbuf_byte_pos];
	    //printf("%40s\n", dec_to_bin_ll(hay));
	    if (inbuf_byte_pos >= 6)
	    {
                for (i = 0; i <= 8; i++)
                {
//		    printf("hay = \t\t\t\t%s\n", dec_to_bin_ll(hay));
    //		    printf("masks[%d] = \t\t\t%s\n", i, dec_to_bin_ll(masks[i]));
    //		    printf("hay & masks[%d] = \t\t%s\n", i, dec_to_bin_ll(hay & masks[i]));
    //		    printf("needle_shifted[%d] = \t\t%s\n", i, dec_to_bin_ll(needle_shifted[i]));

	            if ( (hay & masks[i]) == needle_shifted[i] ) // if needle was found, define correct position
		    {
		        if (i == 0) {
		            position = (inbuf_read_total + inbuf_byte_pos + 1 - 6) * 8;
//			    printf("search_start_bit_of_bz2_block: inbuf_read_total = %llu, inbuf_byte_pos = %llu\n", inbuf_read_total, inbuf_byte_pos);
		        }
		        else if (i > 0 && i <= 8) {
			    position = (inbuf_read_total + inbuf_byte_pos + 1 - 7) * 8 + 8 - i;
			    //printf("search_start_bit_of_bz2_block: position = (inbuf_read_total(%llu) + inbuf_byte_pos(%llu) + 1 - 7) * 8 + 8 - i(%d) = %lld\n", inbuf_read_total, inbuf_byte_pos, i, position);
		        }
		        else {
			    position = (inbuf_read_total + inbuf_byte_pos + 1 - 8) * 8 + 8 - i;
//			    printf("search_start_bit_of_bz2_block: inbuf_read_total = %llu, inbuf_byte_pos = %llu\n", inbuf_read_total, inbuf_byte_pos);
		        }

		        //printf("search_start_bit_of_bz2_block: position = %llu\n", position);
		        hay = 0; // to prevent from finding duplicates
                        //printf("search_start_bit_of_bz2_block: lseek position BEFORE lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET) = %lu, bd->cur_file_offset = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR), bd->cur_file_offset);
                        lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET); // set file offset back to the value which was before call of this function. Because the found position is correct relatively to that file offset.
		        //break;
                        //printf("search_start_bit_of_bz2_block: lseek position BEFORE lseek(ifd, bd->cur_file_offset, SEEK_SET) = %lu, bd->cur_file_offset = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR), bd->cur_file_offset);
                        //printf("search_start_bit_of_bz2_block: lseek position AFTER lseek(bd->in_fd, bd->cur_file_offset, SEEK_SET) = %lu, bd->cur_file_offset = %lu\n", lseek(bd->in_fd, 0, SEEK_CUR), bd->cur_file_offset);
		        //printf("search_start_bit_of_bz2_block: position = %llu bits\n\n", position);
		        return position;
		    }
	        }
	    }
//	    putchar('\n');
	}
	inbuf_read_total += inbuf_read;
        
    }
}


void usage(char * program_name)
{
    printf("Usage: %s --from=\"date\" --to=\"date\" --file=/path/to/file.bz2\n", program_name);
}
