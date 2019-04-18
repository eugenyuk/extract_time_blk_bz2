all: extract_time_blk_bz2.c micro-bunzip.c
	        gcc -w -o extract_time_blk_bz2 extract_time_blk_bz2.c micro-bunzip.c
