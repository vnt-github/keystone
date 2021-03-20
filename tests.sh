for html_format in 2006 2011
do
    # below with 10 only use if 09 month with close present, or you trade from 2nd day
    # for month in 10 11 12
    for month in 11 12
    do
        echo -e "\n" $html_format $month "\n"
        # below to test with final port folio size
        python trade.py $html_format $((month-1)) $month ~/tmp_dir/ ~/stocks/ | adversary | tail -1 | cut -d "$" -f2
        # below to test portolio sizes
        # for portfolio_size in {5..60..5}
        # do
            # echo -e "\n" $html_format $((month-1)) $month ~/tmp_dir $portfolio_size "\n"
            # python trade.py $html_format $((month-1)) $month ~/tmp_dir/ ~/stocks/ $portfolio_size | adversary | tail -1 | cut -d "$" -f2
        # done
    done
done