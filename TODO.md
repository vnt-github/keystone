- pointers
    - take care of the signs like in Earnings (ttm) etc
        - create generic sign handler
    - N/A handler
    - normalize by price
    - handle K M B in the suffix.

- extract data
    - most data from the professors tutorials.
- put data in database
    - hadoop

- configurable extraction pipeline
    - easy to implement new function
    - enable disable existing function
    - do multiprocessing
    - async keyword python

- how'd you use classes here if applicable
- how'd you use decrorators here if applicable
- read scrappy documentation.
- read



- absolute path vs xpath
    - absolute path: /html/body/table[3]/tr[1]/td[1]/table/tr[2]/td[2]
        - faster
    - search contains text and neighbour.
        - more robut to changes in the structure of the html

- itmes: https://towardsdatascience.com/a-minimalist-end-to-end-scrapy-tutorial-part-ii-b917509b73f7
    - experimented with the items pipelines.
    - seperation of concerp paradigm.
        - faster modifiability
        - add pre post processing
        - prefereed structure to save info in files. "export file".
        - db preferrably.
        - we don't want to create a csv
            - creating code to handle csv
            - large data may need to be kept in memory, which data can handle
            - much more from link above.

- benchmarks
    - 2001 all profiles 6 minutes. (6:26.48)

- stocks movement: https://www.youtube.com/watch?v=R3zVC7mFzDA
- http://blog.ditullio.fr/2015/12/24/hadoop-basics-filter-aggregate-sort-mapreduce/

- hadoop fs -rm -r /wordcount/output_pydoop && pydoop script script.py /wordcount/input/ /wordcount/output_pydoop
- hadoop fs -cat /wordcount/output_pydoop/part-r-00000

- file:///C:/Users/asus/Downloads/Pydoop_a_Python_MapReduce_and_HDFS_API_for_Hadoop.pdf
- https://gist.github.com/alexwoolford/996f186c539f05ce1589


