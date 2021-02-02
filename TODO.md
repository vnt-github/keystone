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