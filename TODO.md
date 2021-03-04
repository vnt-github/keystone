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


- include investors average.
- stratergy:
    - volume base
    - ml closes
    - fundamentals
    - combines results from these.

- fundamentals info.
- will the test data be in different html structures format.
    - 2001, 2006, 2011, 2016, all have diff html format.
    - 2016 data is very very different.



- Cash volume: in order to use the bid/ask price that's observed in my files,  good rule of thumb is to limit the number of shares you want to buy/sell to no more than about 0.1% of the daily volume. So if you have $100,000 total to spend and you're going to buy 10 companies, you're buying $10k worth for each company; you should restrict your purchases to companies that have a daily cash volume of $1,000,000 or more
- use relative to benchmakrs.

- F score: https://en.wikipedia.org/wiki/Piotroski_F-score
- G score: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=403180

- LSTM for tactical data

- how to save the data.
    - hadoop as it's for large data.

- what to do with missing data when evaluating not saving?
    - why it is missing?
    - how it will impact our outcomes?
    - https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3668100/
    - MCAR, MAR, MNAR
    - regression(preferred)/multiple imputation, even if its possible because other variables may or may not be enough.
    - pair wise deletion with whatever strategy we use.
    - Mean substitution, Pairwise deletion, listwise deletion, Maximum likelihood estimation (MLE).
    - compeletly missing then remove.


chmod +x run.sh

sshfs -o IdentityFile=~/.ssh/vnt_openlab_uci allow_other,default_permissions,idmap=user vbharot@openlab.ics.uci.edu:/home/vbharot/stocks /mnt/e/vnt/UCI/stocks


sshfs -o IdentityFile=~/.ssh/vnt_openlab_uci allow_other,default_permissions,idmap=user vbharot@openlab.ics.uci.edu:/home/vbharot/stocks ./test_mnt
