# extract_time_blk_bz2

This tool is useful when you need to get a part of a huge log file which was compressed with bzip2 (hundreds of gigabytes). The part of tool, which extracts bz2 blocks was given from the project of James Taylor [seek-bzip2](https://bitbucket.org/james_taylor/seek-bzip2). 

I've added a possibility to extract only that bz2 blocks, which contains a data between --from and --to timestamps in a log.


### How to comile:
`make`


### How to use a tool:
`extract_time_blk_bz2 --from="" --to="" --file=/full/path/to/file.bz2`

Where supported from/to datetime formats are:

- "%Y-%m-%dT%H:%M:%S" (Ex. "2017-02-21T14:53:22")
- "%b %d %H:%M:%S"    (Ex. "Oct 30 05:54:01") 
- "%Y-%m-%d %H:%M:%S" (Ex. "2017-02-21 14:53:22")

### Limitations:
It was used on x64 architecture.
Datatime substring in a log should be at the beginning of a string. For ex.:
`2018-12-02T21:38:29 text text text ...`

`2018-12-02T21:38:30 text text text ...`

